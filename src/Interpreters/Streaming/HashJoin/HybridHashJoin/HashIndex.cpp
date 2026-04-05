#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HashIndex.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HybridHashJoin.h>

namespace DB::Streaming
{

HashIndex::HashIndex(DB::Streaming::HybridHashJoin * join_, std::string_view id_)
    : join(join_), id(id_), current_hash_index(std::make_shared<HybridHashJoinMapsVariants>())
{
}

/// For asof join
HashIndex::HashIndex(
    HybridHashJoin * join_, std::string_view id_, const RangeAsofJoinContext & range_asof_join_ctx_, const String & asof_column_name_)
    : join(join_)
    , id(id_)
    , range_asof_join_ctx(range_asof_join_ctx_)
    , asof_col_name(asof_column_name_)
    , current_hash_index(std::make_shared<HybridHashJoinMapsVariants>())
{
    updateBucketSize();
}

void HashIndex::updateBucketSize()
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
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Invalid range=({}, {})", range_asof_join_ctx.lower_bound, range_asof_join_ctx.upper_bound);
    }
}

void HashIndex::updateAsofJoinColumnPositionAndScale(UInt16 scale, size_t asof_col_pos_, TypeIndex type_index)
{
    range_asof_join_ctx.lower_bound *= intExp10(scale);
    range_asof_join_ctx.upper_bound *= intExp10(scale);
    asof_col_pos = asof_col_pos_;

    updateBucketSize();

    range_splitter = createBlockRangeSplitter(type_index, asof_col_pos, bucket_size, true);
}

size_t HashIndex::removeOldBuckets(std::string_view stream)
{
    Int64 watermark = join->combined_watermark;
    watermark -= bucket_size;

    size_t remaining_bytes = 0;

    std::vector<Int64> buckets_to_remove;
    {
        /// std::scoped_lock lock(mutex);

        for (auto iter = range_bucket_hash_indexes.begin(); iter != range_bucket_hash_indexes.end(); ++iter)
        {
            if (iter->first <= watermark)
            {
                buckets_to_remove.push_back(iter->first);
                iter = range_bucket_hash_indexes.erase(iter);
            }
            else
                break;
        }

        /// Enforce max bucket count: when one stream is idle, the watermark-based
        /// reclamation cannot advance, causing unbounded bucket accumulation.
        /// Remove the oldest excess buckets to cap memory usage.
        auto max_buckets = range_asof_join_ctx.max_buckets;
        if (max_buckets > 0)
        {
            while (range_bucket_hash_indexes.size() > max_buckets)
            {
                auto oldest = range_bucket_hash_indexes.begin();
                buckets_to_remove.push_back(oldest->first);
                range_bucket_hash_indexes.erase(oldest);
            }
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

size_t HashIndex::index(LightChunkWithTimestamp && block)
{
    chassert(current_hash_index);

    /// std::scoped_lock lock(mutex);
    return index(std::move(block), current_hash_index);
}

size_t HashIndex::index([[maybe_unused]] LightChunkWithTimestamp && block, [[maybe_unused]] HybridHashJoinMapsVariantsPtr target_hash_index)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "index(LightChunkWithTimestamp && block, HybridHashJoinMapsVariantsPtr target_hash_index) is not implemented");
}

std::vector<HashIndex::BucketBlock> HashIndex::assignDataBlockToRangeBuckets(Block && block)
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

            HybridHashJoinMapsVariantsPtr target_hash_index = nullptr;
            {
                /// std::scoped_lock lock(mutex);

                if (auto iter = range_bucket_hash_indexes.find(bucket_block.first); iter == range_bucket_hash_indexes.end())
                {
                    auto maps_variants = std::make_shared<HybridHashJoinMapsVariants>();
                    range_bucket_hash_indexes.emplace(
                        bucket_block.first,
                        IndexWithTimestamps{maps_variants, bucket_block.second.minTimestamp(), bucket_block.second.maxTimestamp()});

                    /// Init hash table
                    chassert(join && sample_block);
                    join->initHashMaps(*maps_variants, sample_block, fmt::format("{}-range-bucket-{}", id, bucket_block.first));

                    /// Update watermark
                    if (static_cast<Int64>(bucket_block.first) > current_watermark)
                        current_watermark = bucket_block.first;

                    target_hash_index = maps_variants;
                }
                else
                {
                    iter->second.min_ts = std::min(iter->second.min_ts, bucket_block.second.minTimestamp());
                    iter->second.max_ts = std::max(iter->second.max_ts, bucket_block.second.maxTimestamp());
                    target_hash_index = iter->second.index;
                }
            }

            bucket_assigned_blocks.emplace_back(bucket_block.first, std::move(bucket_block.second), std::move(target_hash_index));
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

size_t HashIndex::approximateCount() const
{
    size_t count = 0;
    auto collect_count = [&](const HybridHashJoinMapsVariants & index) {
        std::vector<const HybridMapsVariant *> maps_vector;
        maps_vector.reserve(index.size());
        for (size_t i = 0; i < index.size(); ++i)
            maps_vector.push_back(&index[i]);

        hybridJoinDispatch(
            join->getStreamingKind(),
            join->getStreamingStrictness(),
            maps_vector,
            [&](auto /*kind_*/, auto /*strictness_*/, const auto & maps_vector_) {
                for (const auto & map : maps_vector_)
                    count += map->table.approximateCount();
            });
    };

    {
        std::scoped_lock lock(mutex);
        collect_count(*current_hash_index);

        for (const auto & [_, index_with_timestamps] : range_bucket_hash_indexes)
            collect_count(*index_with_timestamps.index);
    }

    return count;
}

size_t HashIndex::getBufferSizeInBytes() const
{
    size_t bytes = 0;
    auto collect_bytes = [&](const HybridHashJoinMapsVariants & index) {
        std::vector<const HybridMapsVariant *> maps_vector;
        maps_vector.reserve(index.size());
        for (size_t i = 0; i < index.size(); ++i)
            maps_vector.push_back(&index[i]);

        hybridJoinDispatch(
            join->getStreamingKind(),
            join->getStreamingStrictness(),
            maps_vector,
            [&](auto /*kind_*/, auto /*strictness_*/, const auto & maps_vector_) {
                for (const auto & map : maps_vector_)
                    bytes += map->table.getBufferSizeInBytes();
            });
    };

    {
        std::scoped_lock lock(mutex);
        collect_bytes(*current_hash_index);

        for (const auto & [_, index_with_timestamps] : range_bucket_hash_indexes)
            collect_bytes(*index_with_timestamps.index);
    }

    return bytes;
}
}
