#include <Interpreters/Streaming/HashJoin.h>
#include <Interpreters/Streaming/joinData.h>

#include <Interpreters/JoinUtils.h>

namespace DB
{
namespace Streaming
{
HashBlocks::HashBlocks(JoinMetrics & metrics)
    : blocks(metrics), new_data_iter(blocks.end()), maps(std::make_unique<HashJoinMapsVariants>())
{
    /// FIXME, in some cases, `maps` is not needed
}

HashBlocks::HashBlocks(Block && block, JoinMetrics & metrics) : HashBlocks(metrics)
{
    addBlock(std::move(block));
}

HashBlocks::~HashBlocks() = default;

size_t BufferedStreamData::removeOldBuckets(std::string_view stream)
{
    Int64 watermark = join->combined_watermark;
    watermark -= bucket_size;

    size_t remaining_bytes = 0;

    std::vector<Int64> buckets_to_remove;
    {
        std::scoped_lock lock(mutex);

        for (auto iter = time_bucket_hash_blocks.begin(); iter != time_bucket_hash_blocks.end(); ++iter)
        {
            if (iter->first <= watermark)
            {
                buckets_to_remove.push_back(iter->first);
                iter = time_bucket_hash_blocks.erase(iter);
            }
            else
                break;
        }

        remaining_bytes = metrics.total_bytes;
    }

    if (!buckets_to_remove.empty())
        LOG_INFO(
            join->log,
            "Removing data in time buckets={} in {} stream. Remaining bytes={} blocks={}",
            fmt::join(buckets_to_remove.begin(), buckets_to_remove.end(), ","),
            stream,
            metrics.total_bytes,
            metrics.total_blocks);

    return remaining_bytes;
}

void BufferedStreamData::updateBucketSize()
{
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

    join_start_bucket = 0; /// left_bucket - join_start_bucket * bucket_size
    join_stop_bucket = 0; /// left_bucket + join_stop_bucket * bucket_size

    /// Bucket join: given a left bucket, the right joined blocks can possibly fall in range : [left_bucket - 2 * bucket_size, left_bucket + 2 * bucket_size]

    if (range_asof_join_ctx.upper_bound == 0)
    {
        /// case 1, [left_bucket, left_bucket + 2 * bucket_size]
        join_start_bucket = 0;
        join_stop_bucket = 2;
    }
    else if (range_asof_join_ctx.upper_bound < bucket_size)
    {
        /// case 2, [left_bucket - 1 * bucket_size, left_bucket + 2 * bucket_size]
        join_start_bucket = 1;
        join_stop_bucket = 2;
    }
    else if (range_asof_join_ctx.upper_bound == bucket_size)
    {
        /// case 3, [left_bucket - 1 * bucket_size, left_bucket + 1 * bucket_size]
        join_start_bucket = 1;
        join_stop_bucket = 1;
    }
    else if (range_asof_join_ctx.upper_bound > bucket_size && range_asof_join_ctx.lower_bound < 0)
    {
        /// case 4, [left_bucket - 2 * bucket_size, left_bucket + 1 * bucket_size]
        join_start_bucket = 2;
        join_stop_bucket = 1;
    }
    else if (range_asof_join_ctx.lower_bound == 0) /// we can't use upper_bound == 2 * bucket_size here for case like 0 < left - right < 13
    {
        /// case 5, [left_bucket - 2 * bucket_size, left_bucket]
        join_start_bucket = 2;
        join_stop_bucket = 0;
    }
    else
        assert(0);
}

void BufferedStreamData::updateAsofJoinColumnPositionAndScale(UInt16 scale, size_t asof_col_pos_, TypeIndex type_index)
{
    range_asof_join_ctx.lower_bound *= intExp10(scale);
    range_asof_join_ctx.upper_bound *= intExp10(scale);
    asof_col_pos = asof_col_pos_;

    updateBucketSize();

    range_splitter = createBlockRangeSplitter(type_index, asof_col_pos, bucket_size, true);
}


void BufferedStreamData::addBlock(Block && block)
{
    std::scoped_lock lock(mutex);
    addBlockWithoutLock(std::move(block));
}

void BufferedStreamData::addBlockWithoutLock(Block && block)
{
    assert(current_hash_blocks);

    current_hash_blocks->addBlock(std::move(block));
    setHasNewData(true);
}

void BufferedStreamData::addBlockToTimeBucket(Block && block)
{
    /// Categorize block according to time bucket, then we can prune the time bucketed blocks
    /// when `watermark` passed its time
    auto bucketed_blocks = range_splitter->split(std::move(block));

    std::vector<std::pair<UInt64, size_t>> late_blocks;

    {
        std::scoped_lock lock(mutex);

        for (auto & bucket_block : bucketed_blocks)
        {
            if (static_cast<Int64>(bucket_block.first) + bucket_size < join->combined_watermark)
            {
                late_blocks.emplace_back(bucket_block.first, bucket_block.second.rows());
                continue;
            }

            auto iter = time_bucket_hash_blocks.find(bucket_block.first);
            if (iter != time_bucket_hash_blocks.end())
            {
                iter->second->addBlock(std::move(bucket_block.second));
            }
            else
            {
                time_bucket_hash_blocks.emplace(bucket_block.first, std::make_shared<HashBlocks>(std::move(bucket_block.second), metrics));

                /// Update watermark
                if (static_cast<Int64>(bucket_block.first) > current_watermark)
                    current_watermark = bucket_block.first;
            }

            setHasNewData(true);
        }
    }

    Int64 watermark = join->combined_watermark;
    for (auto [bucket, rows] : late_blocks)
    {
        LOG_INFO(
            join->log,
            "Discard {} late events in time bucket {} of left stream since it is later than latest combined watermark {} with "
            "bucket_size={}",
            rows,
            bucket,
            watermark,
            bucket_size);
    }
}
}
}
