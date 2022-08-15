#pragma once

#include "RangeAsofJoinContext.h"
#include "joinBlockList.h"
#include "joinMetrics.h"
#include "joinTuple.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/SplitBlock.h>
#include <Common/Arena.h>

#include <deque>
#include <map>

namespace DB
{
using RawBlockPtr = const Block *;
using BlockNullmapList = std::deque<std::pair<RawBlockPtr, ColumnPtr>>;

namespace JoinCommon
{
class JoinMask;
}

namespace Streaming
{
class HashJoin;
struct StreamData;
struct HashJoinMapsVariants;

struct RightStreamBlocks
{
    explicit RightStreamBlocks(StreamData * stream_data_);

    void insertBlock(Block && block)
    {
        /// FIXME, there are hashmap memory as well
        blocks.updateMetrics(block);
        blocks.push_back(std::move(block));
        has_new_data_since_last_join = true;
    }

    StreamData * stream_data;
    JoinBlockList blocks;
    std::shared_ptr<HashJoinMapsVariants> maps;
    BlockNullmapList blocks_nullmaps; /// Nullmaps for blocks of "right" table (if needed)

    bool has_new_data_since_last_join = false;

    /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
    Arena pool;

    bool hasNewData() const { return has_new_data_since_last_join; }
    void markNoNewData() { has_new_data_since_last_join = false; }
};
using RightStreamBlocksPtr = std::shared_ptr<RightStreamBlocks>;

struct LeftStreamBlocks
{
    explicit LeftStreamBlocks(StreamData * stream_data_);

    LeftStreamBlocks(Block && block, StreamData * stream_data_);

    void insertBlock(Block && block)
    {
        blocks.updateMetrics(block);

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

    StreamData * stream_data;
    JoinBlockList blocks;

    /// point to the new data node if there is, otherwise blocks.end()
    JoinBlockList::iterator new_data_iter;

    JoinTupleMap joined_rows;

    bool hasNewData() const { return new_data_iter != blocks.end(); }
    void markNoNewData() { new_data_iter = blocks.end(); }
};
using LeftStreamBlocksPtr = std::shared_ptr<LeftStreamBlocks>;

struct StreamData
{
    explicit StreamData(HashJoin * join_) : join(join_) { }

    StreamData(const RangeAsofJoinContext & range_asof_join_ctx_, const String & asof_column_name_, HashJoin * join_)
        : range_asof_join_ctx(range_asof_join_ctx_), asof_col_name(asof_column_name_), join(join_)
    {
        updateBucketSize();
    }

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

    void setHasNewData(bool has_new_data) { has_new_data_since_last_join = has_new_data; }
    bool hasNewData() const { return has_new_data_since_last_join; }

    template <typename V>
    size_t removeOldBuckets(std::map<Int64, V> & blocks, std::string_view stream);

    bool has_new_data_since_last_join = false;
    RangeAsofJoinContext range_asof_join_ctx;
    Int64 bucket_size = 0;
    Int64 join_start_bucket = 0;
    Int64 join_stop_bucket = 0;
    String asof_col_name;
    Int64 asof_col_pos = -1;
    BlockRangeSplitterPtr range_splitter;
    std::atomic_int64_t current_watermark = 0;
    HashJoin * join;

    JoinMetrics metrics;

    std::mutex mutex;
};

struct RightStreamData : public StreamData
{
    using StreamData::StreamData;

    Block sample_block; /// Block as it would appear in the BlockList

    size_t removeOldData();

    std::map<Int64, RightStreamBlocksPtr> hashed_blocks;

    void initCurrentJoinBlocks();

    /// `current_join_blocks` serves 3 purposes
    /// 1) During query plan phase, we will need it to evaluate the header
    /// 2) Workaround the `joinBlock` API interface for range join, it points the current working right blocks in the time bucket
    /// 3) For non-range join, it points the global blocks since there is no time bucket in this case
    RightStreamBlocksPtr current_join_blocks;
};

using RightStreamDataPtr = std::shared_ptr<RightStreamData>;

struct LeftStreamData : public StreamData
{
    using StreamData::StreamData;

    void insertBlock(Block && block);

    void insertBlockToTimeBucket(Block && block);

    size_t removeOldData();

    void initCurrentJoinBlocks();

    JoinTupleMap & getJoinedMap();

    UInt64 block_id = 0;
    std::map<Int64, LeftStreamBlocksPtr> blocks;

    /// `current_join_blocks serves 2 different purpose
    /// 1) Workaround the `joinBlock` interface. For range join, it points to the current working blocks in the time bucket
    /// 2) For all join, it points to the global working blocks since there is not time bucket in this case
    LeftStreamBlocksPtr current_join_blocks;
};

using LeftStreamDataPtr = std::shared_ptr<LeftStreamData>;

}
}
