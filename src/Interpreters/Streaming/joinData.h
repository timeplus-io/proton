#pragma once

#include <Interpreters/Streaming/CachedBlockMetrics.h>
#include <Interpreters/Streaming/RangeAsofJoinContext.h>
#include <Interpreters/Streaming/RefCountDataBlockList.h>
#include <Interpreters/Streaming/joinSerder_fwd.h>
#include <Interpreters/Streaming/joinTuple.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/BlockRangeSplitter.h>
#include <Core/LightChunk.h>
#include <Common/serde.h>
#include <Common/Arena.h>
#include <Common/HashMapSizes.h>
#include <Common/HashMapsTemplate.h>

#include <deque>
#include <map>

namespace DB
{
namespace Streaming
{
struct HashJoinMapsVariants;
class HashJoin;

using JoinDataBlock = LightChunkWithTimestamp;
using JoinDataBlockList = RefCountDataBlockList<JoinDataBlock>;
using JoinDataBlockRawPtr = const JoinDataBlock *;
using BlockNullmapList = std::deque<std::pair<JoinDataBlockRawPtr, ColumnPtr>>;

struct HashBlocks
{
    HashBlocks(size_t data_block_size, CachedBlockMetrics & metrics);

    ~HashBlocks();

    [[nodiscard]] size_t addOrConcatDataBlock(JoinDataBlock && block) { return blocks.pushBackOrConcat(std::move(block)); }

    const JoinDataBlock & lastDataBlock() const { return blocks.lastDataBlock(); }

    HashMapSizes hashMapSizes(const HashJoin * hash_join) const;

    void serialize(
        WriteBuffer & wb,
        VersionType version,
        const Block & header,
        const HashJoin & join,
        SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices) const;

    void deserialize(
        ReadBuffer & rb,
        VersionType version,
        const Block & header,
        const HashJoin & join,
        DeserializedIndicesToRowRefListMultiple<JoinDataBlock> * deserialized_indices_to_multiple_ref);

    /// Buffered data
    JoinDataBlockList blocks;

    /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
    Arena pool;

    /// Hash maps variants are attached to original source blocks, and will be garbage collected
    /// automatically along with the source blocks. Hence put it here instead of BufferedStreamData
    std::unique_ptr<HashJoinMapsVariants> maps;
    BlockNullmapList blocks_nullmaps; /// Nullmaps for blocks of "right" table (if needed)
};
using HashBlocksPtr = std::shared_ptr<HashBlocks>;

SERDE struct BufferedStreamData
{
    explicit BufferedStreamData(HashJoin * join_);

    /// For asof join
    BufferedStreamData(HashJoin * join_, const RangeAsofJoinContext & range_asof_join_ctx_, const String & asof_column_name_);

    /// Add block, assign block id and return block id
    [[nodiscard]] size_t addOrConcatDataBlock(JoinDataBlock && block);
    [[nodiscard]] size_t addOrConcatDataBlockWithoutLock(JoinDataBlock && block, HashBlocksPtr & target_hash_blocks);

    struct BucketBlock
    {
        BucketBlock(size_t bucket_, Block && block_, HashBlocksPtr hash_blocks_)
            : bucket(bucket_), block(std::move(block_)), hash_blocks(std::move(hash_blocks_))
        {
            assert(block.rows());
        }

        HashMapSizes hashMapSizes(const HashJoin * hash_join) const
        {
            return hash_blocks ? hash_blocks->hashMapSizes(hash_join) : HashMapSizes{};
        }

        size_t bucket = 0;
        Block block;
        HashBlocksPtr hash_blocks;
    };
    std::vector<BucketBlock> assignDataBlockToRangeBuckets(Block && block);

    /// Check if [min_ts, max_ts] intersects with range bucket [bucket_start_ts, bucket_start_ts + bucket_size]
    /// The rational behind this is stream data is high temporal, we probably has a good chance to prune the
    /// data up-front before the join
    bool ALWAYS_INLINE intersect(Int64 left_min_ts, Int64 left_max_ts, Int64 right_min_ts, Int64 right_max_ts) const
    {
        assert(left_max_ts >= left_min_ts);
        assert(right_max_ts >= right_min_ts);
        /// left : [left_min_ts, right_max_ts]
        /// right : [right_min_ts, right_max_ts]
        /// lower_bound < left - right < upper_bound
        /// There are 2 cases for non-intersect: iter min/max ts is way bigger or way smaller comparing to right range bucket
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

    size_t removeOldBuckets(std::string_view stream);

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

    const CachedBlockMetrics & getJoinMetrics() const { return metrics; }

    String joinMetricsString() const;

    const auto & getRangeBucketHashBlocks() const { return range_bucket_hash_blocks; }
    auto & getRangeBucketHashBlocks() { return range_bucket_hash_blocks; }

    const HashBlocks & getCurrentHashBlocks() const
    {
        assert(current_hash_blocks);
        return *current_hash_blocks;
    }

    const HashBlocksPtr & getCurrentHashBlocksPtr() const { return current_hash_blocks; }

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

    HashBlocksPtr newHashBlocks();

    HashMapSizes hashMapSizes(const HashJoin * hash_join) const
    {
        if (!range_bucket_hash_blocks.empty())
        {
            HashMapSizes sizes;
            for (const auto & hash_blocks : range_bucket_hash_blocks)
            {
                auto one_sizes = hash_blocks.second->hashMapSizes(join);
                sizes.keys = one_sizes.keys;
                sizes.buffer_size_in_bytes = one_sizes.buffer_size_in_bytes;
                sizes.buffer_bytes_in_cells = one_sizes.buffer_bytes_in_cells;
            }

            return sizes;
        }
        else if (current_hash_blocks)
        {
            return current_hash_blocks->hashMapSizes(join);
        }
        return {};
    }

    void serialize(WriteBuffer & wb, VersionType version, SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices = nullptr) const;
    void deserialize(
        ReadBuffer & rb, VersionType version, DeserializedIndicesToRowRefListMultiple<JoinDataBlock> * deserialized_indices_to_row_ref_list_multiple = nullptr);

    NO_SERDE HashJoin * join;

    RangeAsofJoinContext range_asof_join_ctx;
    Int64 bucket_size = 0;
    Int64 join_start_bucket_offset = 0;
    Int64 join_stop_bucket_offset = 0;
    NO_SERDE String asof_col_name;
    NO_SERDE Int64 asof_col_pos = -1;
    NO_SERDE BlockRangeSplitterPtr range_splitter;
    std::atomic_int64_t current_watermark = 0;

    NO_SERDE Block sample_block; /// Block as it would appear in the BlockList
    NO_SERDE std::optional<std::vector<size_t>>
        reserved_column_positions; /// `_tp_delta` etc column positions in sample block if they exist

    NO_SERDE mutable std::mutex mutex;

private:
    /// Global block id for left or right stream data
    UInt64 block_id = 0;

    CachedBlockMetrics metrics;

    /// `current_hash_blocks` serves 3 purposes
    /// 1) During query plan phase, we will need it to evaluate the header
    /// 2) Workaround the `joinBlock` API interface for range join, it points the current working right blocks in the range bucket
    /// 3) For global join, it points to the global working blocks since there is not range bucket in this case
    HashBlocksPtr current_hash_blocks;

    /// Only for range join
    std::map<Int64, HashBlocksPtr> range_bucket_hash_blocks;
};

using BucketBlocks = std::vector<BufferedStreamData::BucketBlock>;

using BufferedStreamDataPtr = std::unique_ptr<BufferedStreamData>;

template <typename JoinDataBlock, typename MappedRowRef>
struct BufferedHashData
{
    using JoinDataBlockList = RefCountDataBlockList<JoinDataBlock>;
    using HashMap = HashMapsTemplate<MappedRowRef>;

    BufferedHashData(size_t data_block_size) : blocks(data_block_size, metrics) { }
    ~BufferedHashData();

    size_t totalBufferedBytes() const { return metrics.totalBytes() + map.getTotalByteCountImpl(); }

    HashMapSizes hashMapSizes() const
    {
        return HashMapSizes{
            .keys = map.getTotalRowCount(),
            .buffer_size_in_bytes = map.getTotalByteCountImpl(),
            .buffer_bytes_in_cells = map.getBufferSizeInCells(),
        };
    }

    String getMetricsString() const
    {
        return fmt::format("cached block list: ({}), hash map: ({})", metrics.string(), hashMapSizes().string());
    }

    template <typename RowRefHandler>
    void insert(Block && block, RowRefHandler && row_ref_handler)
    {
        auto rows = block.rows();
        auto start_row = blocks.pushBackOrConcat(std::move(block));
        right_buffered_hash_data->map.insert(std::move(block_to_save), key_columns, key_sizes, rows, right_buffered_hash_data->pool, std::move(row_ref_handler));
    }

    /// Buffered data
    CachedBlockMetrics metrics;
    JoinDataBlockList blocks;

    /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
    Arena pool;

    /// Hash maps variants are attached to original source blocks, and will be garbage collected
    /// automatically along with the source blocks. Hence put it here instead of BufferedStreamData
    HashMap map;

    /// FIXME: support nullable type
    // using JoinDataBlockRawPtr = const JoinDataBlock *;
    // using BlockNullmapList = std::deque<std::pair<JoinDataBlockRawPtr, ColumnPtr>>;
    // BlockNullmapList blocks_nullmaps; /// Nullmaps for blocks of "right" table (if needed)
};

}
}
