#pragma once

#include <Core/BlockRangeSplitter.h>
#include <Interpreters/Streaming/HashJoin/CachedBlockMetrics.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/hybridJoinDispatch.h>
#include <Common/serde.h>

namespace DB::Streaming
{
class HybridHashJoin;

SERDE struct HashIndex
{
    explicit HashIndex(HybridHashJoin * join_, std::string_view id_);

    /// For range join
    HashIndex(
        HybridHashJoin * join_, std::string_view id_, const RangeAsofJoinContext & range_asof_join_ctx_, const String & asof_column_name_);

    size_t index(LightChunkWithTimestamp && block);
    size_t index(LightChunkWithTimestamp && block, HybridHashJoinMapsVariantsPtr target_hash_index);

    void updateBucketSize();
    size_t removeOldBuckets(std::string_view stream);
    void updateAsofJoinColumnPositionAndScale(UInt16 scale, size_t asof_col_pos_, TypeIndex type_index);

    struct BucketBlock
    {
        BucketBlock(size_t bucket_, Block && block_, HybridHashJoinMapsVariantsPtr maps_variants_)
            : bucket(bucket_), block(std::move(block_)), maps_variants(std::move(maps_variants_))
        {
            chassert(block.rows() != 0);
        }

        size_t bucket = 0;
        Block block;
        HybridHashJoinMapsVariantsPtr maps_variants;
    };
    std::vector<BucketBlock> assignDataBlockToRangeBuckets(Block && block);

    /// Check if [min_ts, max_ts] intersects with range bucket [bucket_start_ts, bucket_start_ts + bucket_size]
    /// The rational behind this is stream data is high temporal, we probably has a good chance to prune the
    /// data up-front before the join
    bool ALWAYS_INLINE intersect(Int64 left_min_ts, Int64 left_max_ts, Int64 right_min_ts, Int64 right_max_ts) const
    {
        chassert(left_max_ts >= left_min_ts);
        chassert(right_max_ts >= right_min_ts);
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

    struct IndexWithTimestamps
    {
        IndexWithTimestamps(HybridHashJoinMapsVariantsPtr index_, int64_t min_ts_, int64_t max_ts_)
            : index(std::move(index_)), min_ts(min_ts_), max_ts(max_ts_)
        {
        }

        HybridHashJoinMapsVariantsPtr index;
        int64_t min_ts = std::numeric_limits<int64_t>::max();
        int64_t max_ts = std::numeric_limits<int64_t>::min();
    };

    const auto & getRangeBucketHashIndexes() const { return range_bucket_hash_indexes; }
    auto & getRangeBucketHashIndexes() { return range_bucket_hash_indexes; }

    const HybridHashJoinMapsVariants & getCurrentMapsVariants() const
    {
        chassert(current_hash_index);
        return *current_hash_index;
    }

    HybridHashJoinMapsVariants & getCurrentMapsVariants()
    {
        chassert(current_hash_index);
        return *current_hash_index;
    }

    size_t approximateCount() const;
    size_t getBufferSizeInBytes() const;

    String metricsString() const;

    void serialize(WriteBuffer & wb, VersionType version) const;
    void deserialize(ReadBuffer & rb, VersionType version);

    void write(RocksDBColumnFamilyHandlerPtr cf_handler, VersionType version);
    void read(RocksDBColumnFamilyHandlerPtr cf_handler, VersionType version);

    NO_SERDE HybridHashJoin * join;
    NO_SERDE std::string id;

    NO_SERDE Block sample_block;
    RangeAsofJoinContext range_asof_join_ctx;
    Int64 bucket_size = 0;
    Int64 join_start_bucket_offset = 0;
    Int64 join_stop_bucket_offset = 0;
    NO_SERDE String asof_col_name;
    NO_SERDE Int64 asof_col_pos = -1;
    NO_SERDE BlockRangeSplitterPtr range_splitter;
    std::atomic_int64_t current_watermark = 0;

    CachedBlockMetrics metrics;

    NO_SERDE mutable std::mutex mutex;

private:
    /// `current_hash_blocks` serves 3 purposes
    /// 1) During query plan phase, we will need it to evaluate the header
    /// 2) Workaround the `joinBlock` API interface for range join, it points the current working right blocks in the range bucket
    /// 3) For global join, it points to the global working blocks since there is not range bucket in this case
    HybridHashJoinMapsVariantsPtr current_hash_index;

    /// Only for range join
    std::map<Int64, IndexWithTimestamps> range_bucket_hash_indexes;
};

using HashIndexPtr = std::shared_ptr<HashIndex>;

}
