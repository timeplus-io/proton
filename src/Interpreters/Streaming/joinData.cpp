#include <Interpreters/Streaming/HashJoin.h>
#include <Interpreters/Streaming/joinData.h>

#include <Interpreters/JoinUtils.h>

namespace DB
{
namespace Streaming
{
HashBlocks::HashBlocks(size_t data_block_size, CachedBlockMetrics & metrics)
    : blocks(data_block_size, metrics), maps(std::make_unique<HashJoinMapsVariants>())
{
}

HashBlocks::~HashBlocks() = default;

HashMapSizes HashBlocks::hashMapSizes(const DB::Streaming::HashJoin * hash_join) const
{
    return maps->sizes(hash_join);
}

void HashBlocks::serialize(
    WriteBuffer & wb,
    VersionType version,
    const Block & header,
    const HashJoin & join,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices) const
{
    assert(maps);
    serializeHashJoinMapsVariants(blocks, *maps, wb, version, header, join, serialized_row_ref_list_multiple_to_indices);

    /// FIXME: Useless for now
    // BlockNullmapList blocks_nullmaps;
}

void HashBlocks::deserialize(
    ReadBuffer & rb,
    VersionType version,
    const Block & header,
    const HashJoin & join,
    DeserializedIndicesToRowRefListMultiple<JoinDataBlock> * deserialized_indices_to_multiple_ref)
{
    assert(maps);
    deserializeHashJoinMapsVariants(blocks, *maps, rb, version, pool, header, join, deserialized_indices_to_multiple_ref);

    /// FIXME: Useless for now
    // BlockNullmapList blocks_nullmaps;
}

BufferedStreamData::BufferedStreamData(HashJoin * join_)
    : join(join_), current_hash_blocks(std::make_shared<HashBlocks>(join->dataBlockSize(), metrics))
{
}

/// For asof join
BufferedStreamData::BufferedStreamData(
    HashJoin * join_, const RangeAsofJoinContext & range_asof_join_ctx_, const String & asof_column_name_)
    : join(join_)
    , range_asof_join_ctx(range_asof_join_ctx_)
    , asof_col_name(asof_column_name_)
    , current_hash_blocks(std::make_shared<HashBlocks>(join->dataBlockSize(), metrics))
{
    updateBucketSize();
}

size_t BufferedStreamData::removeOldBuckets(std::string_view stream)
{
    Int64 watermark = join->combined_watermark;
    watermark -= bucket_size;

    size_t remaining_bytes = 0;

    std::vector<Int64> buckets_to_remove;
    {
        /// std::scoped_lock lock(mutex);

        for (auto iter = range_bucket_hash_blocks.begin(); iter != range_bucket_hash_blocks.end(); ++iter)
        {
            if (iter->first <= watermark)
            {
                buckets_to_remove.push_back(iter->first);
                iter = range_bucket_hash_blocks.erase(iter);
            }
            else
                break;
        }

        remaining_bytes = metrics.totalBytes();
    }

    if (!buckets_to_remove.empty())
        LOG_INFO(
            join->logger,
            "Removing data in range buckets={} in {} stream. Remaining bytes={} blocks={}",
            fmt::join(buckets_to_remove.begin(), buckets_to_remove.end(), ","),
            stream,
            metrics.totalBytes(),
            metrics.total_blocks);

    return remaining_bytes;
}

void BufferedStreamData::updateBucketSize()
{
    /// We split the range to 2 half. Examples:
    /// `date_diff_within(10)` => [-10, 10] => bucket_size = 10
    bucket_size = (range_asof_join_ctx.upper_bound - range_asof_join_ctx.lower_bound + 1) / 2;

    /// Given a left_bucket (base bucket 0), calculate the possible right buckets to join
    /// Note
    /// 1. We have always transformed the join expression in form of `left.column - right.column`,
    /// like date_diff('second', left._tp_time, right._tp_time)
    /// 2. range_join_ctx.lower_bound <= 0
    /// 3. range_join_ctx.upper_bound >= 0
    /// There are several cases in the following forms.
    /// 1. -20 < left.column - right.column < 0 (left column is less than right column), right buckets to join [0, 1, 2]
    /// 2. -18 < left.column - right.column < 2 (left column is generally less than right column), right buckets to join [-1, 0, 1, 2]
    /// 3. -10 < left.column - right.column < 10 (left, right column is generally in the same range), right buckets to join [-1, 0, 1]
    /// 4. -2 < left.column - right.column < 18 (left column is generally greater than right column), right buckets to join [-2, -1, 0, 1]
    /// 5. 0 < left.column - right.column < 20 (left column is greater than right column), right buckets to join [-2, -1, 0]

    join_start_bucket_offset = 0; /// left_bucket - join_start_bucket * bucket_size
    join_stop_bucket_offset = 0; /// left_bucket + join_stop_bucket * bucket_size

    /// Bucket join: given a left bucket, the right joined blocks can possibly fall
    /// in range : [left_bucket - 2 * bucket_size, left_bucket + 2 * bucket_size]
    /// But we can do better to join less buckets with the following :
    /// join_start_bucket_offset, join_stop_bucket_offset calculation
    /// right_buckets = [left_bucket - join_start_bucket_offset, left_bucket + join_stop_bucket_offset]

    /// On other other handle, given a right bucket, the left joined blocks can possibly fall in range
    /// left_buckets = [right_bucket - join_stop_bucket_offset, right_bucket + join_start_bucket_offset]

    if (range_asof_join_ctx.upper_bound == 0)
    {
        /// case 1: -20 < left.column - right.column < 0 (left column is less than right column), right buckets to join [0, 1, 2]
        /// right_buckets = [left_bucket, left_bucket + 2 * bucket_size]
        /// left_buckets = [right_bucket - 2 * bucket_size, left_bucket]
        join_start_bucket_offset = 0;
        join_stop_bucket_offset = 2 * bucket_size;
    }
    else if (range_asof_join_ctx.upper_bound < bucket_size)
    {
        /// case 2: -18 < left.column - right.column < 2 (left column is generally less than right column), right buckets to join [-1, 0, 1, 2]
        /// right_buckets = [left_bucket - 1 * bucket_size, left_bucket + 2 * bucket_size]
        /// left_buckets = [right_bucket - 2 * bucket_size, right_bucket + bucket_size]
        join_start_bucket_offset = bucket_size;
        join_stop_bucket_offset = 2 * bucket_size;
    }
    else if (range_asof_join_ctx.upper_bound == bucket_size)
    {
        /// case 3: -10 < left.column - right.column < 10 (left, right column is generally in the same range), right buckets to join [-1, 0, 1]
        /// right_buckets = [left_bucket - 1 * bucket_size, left_bucket + 1 * bucket_size]
        /// left_buckets = [right_bucket - 1 * bucket_size, right_bucket + 1 * bucket_size]
        join_start_bucket_offset = bucket_size;
        join_stop_bucket_offset = bucket_size;
    }
    else if (range_asof_join_ctx.upper_bound > bucket_size && range_asof_join_ctx.lower_bound < 0)
    {
        /// case 4: -2 < left.column - right.column < 18 (left column is generally greater than right column), right buckets to join [-2, -1, 0, 1]
        /// right_buckets = [left_bucket - 2 * bucket_size, left_bucket + 1 * bucket_size]
        /// left_buckets = [right_bucket - bucket_size, right_bucket + 2 * bucket_size]
        join_start_bucket_offset = 2 * bucket_size;
        join_stop_bucket_offset = bucket_size;
    }
    else if (range_asof_join_ctx.lower_bound == 0) /// we can't use upper_bound == 2 * bucket_size here for case like 0 < left - right < 13
    {
        /// case 5: 0 < left.column - right.column < 20 (left column is greater than right column), right buckets to join [-2, -1, 0]
        /// right_buckets = [left_bucket - 2 * bucket_size, left_bucket]
        /// left_buckets = [right_bucket, right_bucket + 2 * bucket_size]
        join_start_bucket_offset = 2 * bucket_size;
        join_stop_bucket_offset = 0;
    }
    else
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Invalid range=({}, {})", range_asof_join_ctx.lower_bound, range_asof_join_ctx.upper_bound);
}

void BufferedStreamData::updateAsofJoinColumnPositionAndScale(UInt16 scale, size_t asof_col_pos_, TypeIndex type_index)
{
    range_asof_join_ctx.lower_bound *= intExp10(scale);
    range_asof_join_ctx.upper_bound *= intExp10(scale);
    asof_col_pos = asof_col_pos_;

    updateBucketSize();

    range_splitter = createBlockRangeSplitter(type_index, asof_col_pos, bucket_size, true);
}


size_t BufferedStreamData::addOrConcatDataBlock(JoinDataBlock && block)
{
    assert(current_hash_blocks);

    /// std::scoped_lock lock(mutex);
    return addOrConcatDataBlockWithoutLock(std::move(block), current_hash_blocks);
}

size_t BufferedStreamData::addOrConcatDataBlockWithoutLock(JoinDataBlock && block, HashBlocksPtr & target_hash_blocks)
{
    return target_hash_blocks->addOrConcatDataBlock(std::move(block));
}

std::vector<BufferedStreamData::BucketBlock> BufferedStreamData::assignDataBlockToRangeBuckets(Block && block)
{
    /// Categorize block according to range bucket, then we can prune the range bucketed blocks
    /// when `watermark` passed its time. RangeSplitter assign min/max timestamp for each split block
    auto bucket_blocks = range_splitter->split(std::move(block));

    std::vector<std::pair<UInt64, size_t>> late_blocks;

    /// Assign bucket blocks to each hash bucket
    std::vector<BucketBlock> bucket_assigned_blocks;
    bucket_assigned_blocks.reserve(bucket_blocks.size());
    {
        for (auto & bucket_block : bucket_blocks)
        {
            if (static_cast<Int64>(bucket_block.first) + bucket_size < join->combined_watermark)
            {
                late_blocks.emplace_back(bucket_block.first, bucket_block.second.rows());
                continue;
            }

            HashBlocksPtr target_hash_bucket = nullptr;
            {
                /// std::scoped_lock lock(mutex);

                auto iter = range_bucket_hash_blocks.find(bucket_block.first);
                if (iter == range_bucket_hash_blocks.end())
                {
                    std::tie(iter, std::ignore) = range_bucket_hash_blocks.emplace(bucket_block.first, newHashBlocks());

                    /// Init hash table
                    join->initHashMaps(iter->second->maps->map_variants);

                    /// Update watermark
                    if (static_cast<Int64>(bucket_block.first) > current_watermark)
                        current_watermark = bucket_block.first;
                }
                target_hash_bucket = iter->second;
            }

            bucket_assigned_blocks.emplace_back(bucket_block.first, std::move(bucket_block.second), std::move(target_hash_bucket));
        }
    }

    Int64 watermark = join->combined_watermark;
    for (auto [bucket, rows] : late_blocks)
    {
        LOG_INFO(
            join->logger,
            "Discard {} late events in range bucket {} of left stream since it is later than latest combined watermark {} with "
            "bucket_size={}",
            rows,
            bucket,
            watermark,
            bucket_size);
    }

    return bucket_assigned_blocks;
}

String BufferedStreamData::joinMetricsString() const
{
    size_t total_buckets = range_bucket_hash_blocks.size() + (current_hash_blocks ? 1 : 0);
    size_t total_blocks_cached = 0;
    size_t total_blocks_bytes_cached = 0;
    size_t total_blocks_allocated_bytes_cached = 0;
    size_t total_blocks_rows_cached = 0;
    size_t total_blocks_nullmaps_cached = 0;
    size_t total_arena_bytes = 0;
    size_t total_arena_chunks = 0;

    auto accumulateOneHashBlocks = [&](auto & hashed_blocks) {
        assert(hashed_blocks);
        total_blocks_cached += hashed_blocks->blocks.size();
        for (const auto & block : hashed_blocks->blocks)
        {
            total_blocks_rows_cached += block.block.rows();
            total_blocks_bytes_cached += block.block.allocatedBytes();
            total_blocks_allocated_bytes_cached += block.block.allocatedBytes();
        }

        total_blocks_nullmaps_cached += hashed_blocks->blocks_nullmaps.size();

        total_arena_bytes += hashed_blocks->pool.size();
        total_arena_chunks += hashed_blocks->pool.numOfChunks();
    };

    for (const auto & [_, hashed_blocks] : range_bucket_hash_blocks)
        accumulateOneHashBlocks(hashed_blocks);

    if (current_hash_blocks)
        accumulateOneHashBlocks(current_hash_blocks);

    return fmt::format(
        "total_buckets={}, total_blocks_cached={}, total_blocks_bytes_cached={}, total_blocks_allocated_bytes_cached={}, "
        "total_blocks_rows_cached={}, total_blocks_nullmaps_cached={}, total_arena_bytes={}, total_arena_chunks={}, hash_map_sizes={{{}}} "
        "recorded_join_metrics={{{}}}, next_block_id={}",
        total_buckets,
        total_blocks_cached,
        total_blocks_bytes_cached,
        total_blocks_allocated_bytes_cached,
        total_blocks_rows_cached,
        total_blocks_nullmaps_cached,
        total_arena_bytes,
        total_arena_chunks,
        hashMapSizes(join).string(),
        metrics.string(),
        block_id);
}

void BufferedStreamData::serialize(
    WriteBuffer & wb, VersionType version, SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices) const
{
    std::scoped_lock lock(mutex);

    range_asof_join_ctx.serialize(wb);

    DB::writeIntBinary(bucket_size, wb);
    DB::writeIntBinary(join_start_bucket_offset, wb);
    DB::writeIntBinary(join_stop_bucket_offset, wb);

    DB::writeIntBinary(current_watermark.load(), wb);

    DB::writeIntBinary(block_id, wb);

    assert(current_hash_blocks);
    current_hash_blocks->serialize(wb, version, sample_block, *join, serialized_row_ref_list_multiple_to_indices);

    DB::writeIntBinary<UInt32>(static_cast<UInt32>(range_bucket_hash_blocks.size()), wb);
    for (const auto & [bucket, hash_blocks] : range_bucket_hash_blocks)
    {
        DB::writeIntBinary(bucket, wb);
        assert(hash_blocks);
        hash_blocks->serialize(wb, version, sample_block, *join, serialized_row_ref_list_multiple_to_indices);
    }

    if (version <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
        metrics.serialize(wb, version);
}

void BufferedStreamData::deserialize(
    ReadBuffer & rb, VersionType version, DeserializedIndicesToRowRefListMultiple<JoinDataBlock> * deserialized_indices_to_row_ref_list_multiple)
{
    std::scoped_lock lock(mutex);

    range_asof_join_ctx.deserialize(rb);

    DB::readIntBinary(bucket_size, rb);
    DB::readIntBinary(join_start_bucket_offset, rb);
    DB::readIntBinary(join_stop_bucket_offset, rb);

    int64_t recovered_watermark;
    DB::readIntBinary(recovered_watermark, rb);
    current_watermark = recovered_watermark;

    DB::readIntBinary(block_id, rb);

    assert(current_hash_blocks);
    current_hash_blocks->deserialize(rb, version, sample_block, *join, deserialized_indices_to_row_ref_list_multiple);

    UInt32 size;
    Int64 bucket;
    DB::readIntBinary<UInt32>(size, rb);
    for (UInt32 i = 0; i < size; ++i)
    {
        DB::readIntBinary(bucket, rb);
        auto [iter, inserted] = range_bucket_hash_blocks.emplace(bucket, newHashBlocks());
        assert(inserted);
        /// Init hash table
        join->initHashMaps(iter->second->maps->map_variants);
        iter->second->deserialize(rb, version, sample_block, *join, deserialized_indices_to_row_ref_list_multiple);
    }

    if (version <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
        metrics.deserialize(rb, version);
}

HashBlocksPtr BufferedStreamData::newHashBlocks()
{
    return std::make_shared<HashBlocks>(join->dataBlockSize(), metrics);
}
}
}
