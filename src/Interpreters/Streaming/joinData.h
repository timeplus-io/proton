#pragma once

#include <Interpreters/Streaming/RangeAsofJoinContext.h>
#include <Interpreters/Streaming/joinBlockList.h>
#include <Interpreters/Streaming/joinMetrics.h>
#include <Interpreters/Streaming/joinTuple.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/BlockRangeSplitter.h>
#include <Common/Arena.h>

#include <deque>
#include <map>

namespace DB
{
using RawBlockPtr = const Block *;
using BlockNullmapList = std::deque<std::pair<RawBlockPtr, ColumnPtr>>;

namespace Streaming
{
struct HashJoinMapsVariants;

struct HashBlocks
{
    HashBlocks(JoinMetrics & metrics);
    explicit HashBlocks(Block && block, JoinMetrics & metrics);

    ~HashBlocks();

    void addBlock(Block && block)
    {
        block.info.setBlockID(block_id++);

        if (new_data_iter != blocks.end())
        {
            /// new_data_iter already points the earliest new data node
            blocks.push_back(std::move(block));
        }
        else
        {
            blocks.push_back(std::move(block));

            /// point to the new block
            new_data_iter = --blocks.end();
        }
    }

    bool hasNewData() const { return new_data_iter != blocks.end(); }
    void markNoNewData() { new_data_iter = blocks.end(); }

    const Block * lastBlock() const { return blocks.lastBlock(); }

    UInt64 block_id = 0;

    /// Buffered data
    JoinBlockList blocks;

    /// Point to the new data node if there is, otherwise blocks.end()
    /// Since in asof join / version_kv join version_kv scenario,
    /// blocks will be garbage collected, so new_data_iter may be invalidated by GC.
    /// But in such scenarios, we don't need access this iterator
    JoinBlockList::iterator new_data_iter;

    /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
    Arena pool;

    std::unique_ptr<HashJoinMapsVariants> maps;
    BlockNullmapList blocks_nullmaps; /// Nullmaps for blocks of "right" table (if needed)

    JoinTupleMap joined_rows;
};
using HashBlocksPtr = std::shared_ptr<HashBlocks>;

class HashJoin;
struct BufferedStreamData
{
    explicit BufferedStreamData(HashJoin * join_) : join(join_), current_hash_blocks(std::make_shared<HashBlocks>(metrics)) { }

    /// For asof join
    BufferedStreamData(HashJoin * join_, const RangeAsofJoinContext & range_asof_join_ctx_, const String & asof_column_name_)
        : join(join_)
        , range_asof_join_ctx(range_asof_join_ctx_)
        , asof_col_name(asof_column_name_)
        , current_hash_blocks(std::make_shared<HashBlocks>(metrics))
    {
        updateBucketSize();
    }

    void addBlock(Block && block);
    void addBlockWithoutLock(Block && block);
    void addBlockToTimeBucket(Block && block);

    /// Check if [min_ts, max_ts] intersects with time bucket [bucket_start_ts, bucket_start_ts + bucket_size]
    /// The rational behind this is stream data is high temporal, we probably has a good chance to prune the
    /// data up-front before the join
    bool ALWAYS_INLINE intersect(Int64 left_min_ts, Int64 left_max_ts, Int64 right_min_ts, Int64 right_max_ts) const
    {
        assert(left_min_ts > 0 && left_max_ts >= left_min_ts);
        assert(right_min_ts > 0 && right_max_ts >= right_min_ts);
        /// left : [left_min_ts, right_max_ts]
        /// right : [right_min_ts, right_max_ts]
        /// lower_bound < left - right < upper_bound
        /// There are 2 cases for non-intersect: iter min/max ts is way bigger or way smaller comparing to right time bucket
        /// We can consider left inequality and right inequality to accurately prune non-intersected block,
        /// but it is ok here as long as we don't miss any data. And since most of the time,
        /// the timestamp subtraction is probably not aligned with lower_bound / upper bound, it is simpler / more efficient
        /// to just loose the check here
        return !(
            ((left_max_ts - right_min_ts) < range_asof_join_ctx.lower_bound)
            || (left_min_ts - right_max_ts > range_asof_join_ctx.upper_bound));
    }

    void updateAsofJoinColumnPositionAndScale(UInt16 scale, size_t asof_col_pos_, TypeIndex type_index);

    void updateBucketSize();

    void setHasNewData(bool has_new_data)
    {
        has_new_data_since_last_join = has_new_data;

        if (!has_new_data)
            current_hash_blocks->markNoNewData();
    }

    bool hasNewData() const { return has_new_data_since_last_join; }

    size_t removeOldBuckets(std::string_view stream);

    JoinTupleMap & getCurrentJoinedMap()
    {
        assert(current_hash_blocks);
        return current_hash_blocks->joined_rows;
    }

    const HashJoinMapsVariants & getCurrentMapsVariants() const
    {
        assert(current_hash_blocks);
        return *current_hash_blocks->maps;
    }

    HashJoinMapsVariants & getCurrentMapsVariants()
    {
        assert(current_hash_blocks);
        return *current_hash_blocks->maps;
    }

    const JoinMetrics & getJoinMetrics() const { return metrics; }

    const auto & getTimeBucketHashBlocks() const { return time_bucket_hash_blocks; }
    auto & getTimeBucketHashBlocks() { return time_bucket_hash_blocks; }

    const HashBlocks & getCurrentHashBlocks() const
    {
        assert(current_hash_blocks);
        return *current_hash_blocks;
    }

    HashBlocks & getCurrentHashBlocks()
    {
        assert(current_hash_blocks);
        return *current_hash_blocks;
    }

    void resetCurrentHashBlocks(HashBlocksPtr new_current)
    {
        assert(new_current);
        current_hash_blocks = new_current;
    }

    HashBlocksPtr newHashBlocks() { return std::make_shared<HashBlocks>(metrics); }

    HashJoin * join;

    /// Fast boolean to check if there are new data
    /// For range join (time bucket) case, we don't need loop the `time_bucket_hashed_blocks`
    /// to answer this inquery
    bool has_new_data_since_last_join = false;

    RangeAsofJoinContext range_asof_join_ctx;
    Int64 bucket_size = 0;
    Int64 join_start_bucket = 0;
    Int64 join_stop_bucket = 0;
    String asof_col_name;
    Int64 asof_col_pos = -1;
    BlockRangeSplitterPtr range_splitter;
    std::atomic_int64_t current_watermark = 0;

    Block sample_block; /// Block as it would appear in the BlockList

    std::mutex mutex;

private:
    JoinMetrics metrics;

    /// `current_hash_blocks` serves 3 purposes
    /// 1) During query plan phase, we will need it to evaluate the header
    /// 2) Workaround the `joinBlock` API interface for range join, it points the current working right blocks in the time bucket
    /// 3) For non-range join, it points the global blocks since there is no time bucket in this case
    /// 4) For global join, it points to the global working blocks since there is not time bucket in this case
    HashBlocksPtr current_hash_blocks;

    std::map<Int64, HashBlocksPtr> time_bucket_hash_blocks;
};

using BufferedStreamDataPtr = std::unique_ptr<BufferedStreamData>;
}
}
