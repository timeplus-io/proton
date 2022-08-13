#pragma once

#include "JoinStreamDescription.h"
#include "join_tuple.h"

#include <Core/SplitBlock.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/ColumnUtils.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace JoinCommon
{
    class JoinMask;
}

/** Data structure for implementation of Streaming JOIN which is always kind of ASOF join since its last join key
  * is always inequality of timestamps. This implementation is a copy of existing HashJoin and adapt to streaming scenario.
  * This implementation is a copy of existing HashJoin and adapted to streaming scenario. The following comments are copied
  * as well which shall be revised accordingly
  *
  * It is just a hash table: keys -> rows of joined ("right") table.
  * Additionally, CROSS JOIN is supported: instead of hash table, it use just set of blocks without keys.
  *
  * JOIN-s could be only
  * - ASOF x LEFT/INNER
  *
  * ALL means usual JOIN, when rows are multiplied by number of matching rows from the "right" table.
  * ANY uses one line per unique key from right table. For LEFT JOIN it would be any row (with needed joined key) from the right table,
  * for RIGHT JOIN it would be any row from the left table and for INNER one it would be any row from right and any row from left.
  * SEMI JOIN filter left table by keys that are present in right table for LEFT JOIN, and filter right table by keys from left table
  * for RIGHT JOIN. In other words SEMI JOIN returns only rows which joining keys present in another table.
  * ANTI JOIN is the same as SEMI JOIN but returns rows with joining keys that are NOT present in another table.
  * SEMI/ANTI JOINs allow to get values from both tables. For filter table it gets any row with joining same key. For ANTI JOIN it returns
  * defaults other table columns.
  * ASOF JOIN is not-equi join. For one key column it finds nearest value to join according to join inequality.
  * It's expected that ANY|SEMI LEFT JOIN is more efficient that ALL one.
  *
  * If INNER is specified - leave only rows that have matching rows from "right" table.
  * If LEFT is specified - in case when there is no matching row in "right" table, fill it with default values instead.
  * If RIGHT is specified - first process as INNER, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  * If FULL is specified - first process as LEFT, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  *
  * Thus, LEFT and RIGHT JOINs are not symmetric in terms of implementation.
  *
  * All JOINs (except CROSS) are done by equality condition on keys (equijoin).
  * Non-equality and other conditions are not supported.
  *
  * Implementation:
  *
  * 1. Build hash table in memory from "right" table.
  * This hash table is in form of keys -> row in case of ANY or keys -> [rows...] in case of ALL.
  * This is done in insertFromBlock method.
  *
  * 2. Process "left" table and join corresponding rows from "right" table by lookups in the map.
  * This is done in joinBlock methods.
  *
  * In case of ANY LEFT JOIN - form new columns with found values or default values.
  * This is the most simple. Number of rows in left table does not change.
  *
  * In case of ANY INNER JOIN - form new columns with found values,
  *  and also build a filter - in what rows nothing was found.
  * Then filter columns of "left" table.
  *
  * In case of ALL ... JOIN - form new columns with all found rows,
  *  and also fill 'offsets' array, describing how many times we need to replicate values of "left" table.
  * Then replicate columns of "left" table.
  *
  * How Nullable keys are processed:
  *
  * NULLs never join to anything, even to each other.
  * During building of map, we just skip keys with NULL value of any component.
  * During joining, we simply treat rows with any NULLs in key as non joined.
  *
  * Default values for outer joins (LEFT, RIGHT, FULL):
  *
  * Behaviour is controlled by 'join_use_nulls' settings.
  * If it is false, we substitute (global) default value for the data type, for non-joined rows
  *  (zero, empty string, etc. and NULL for Nullable data types).
  * If it is true, we always generate Nullable column and substitute NULLs for non-joined rows,
  *  as in standard SQL.
  */
class StreamingHashJoin : public IJoin
{
public:
    StreamingHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        JoinStreamDescription left_join_stream_desc_,
        JoinStreamDescription right_join_stream_desc_);

    ~StreamingHashJoin() noexcept override;

    /// proton : starts
    /// returns total bytes in left stream cache
    size_t insertLeftBlock(Block left_input_block);

    /// returns total bytes in right stream cache
    size_t insertRightBlock(Block right_input_block);

    Block joinBlocks(size_t & left_cached_bytes, size_t & right_cached_bytes);

    bool needBufferLeftStream() const { return right_stream_desc.hash_semantic != HashSemantic::VersionedKV; }

    bool isRightStreamDataRefCounted() const { return right_stream_desc.hash_semantic == HashSemantic::VersionedKV || right_stream_desc.hash_semantic == HashSemantic::ChangeLogKV; }

    UInt64 keepVersions() const { return right_stream_desc.keep_versions; }

    /// proton : ends

    const TableJoin & getTableJoin() const override { return *table_join; }

    /** Add block of data from right hand of JOIN to the map.
      * Returns false, if some limit was exceeded and you should not insert more data.
      */
    bool addJoinedBlock(const Block & block, bool check_limits) override;

    void checkTypesOfKeys(const Block & block) const override;

    /** Join data from the map (that was previously built by calls to addJoinedBlock) to the block with data from "left" table.
      * Could be called from different threads in parallel.
      */
    void joinBlock(Block & block, ExtraBlockPtr & not_processed) override;

    /** Keep "totals" (separate part of dataset, see WITH TOTALS) to use later.
      */
    void setTotals(const Block & block) override { totals = block; }
    const Block & getTotals() const override { return totals; }

    bool isFilled() const override { return false; }

    /** For RIGHT and FULL JOINs.
      * A stream that will contain default values from left table, joined with rows from right table, that was not joined before.
      * Use only after all calls to joinBlock was done.
      * left_sample_block is passed without account of 'use_nulls' setting (columns will be converted to Nullable inside).
      */
    std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    /// Number of keys in all built JOIN maps.
    size_t getTotalRowCount() const final;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
    size_t getTotalByteCount() const final;

    bool alwaysReturnsEmptySet() const final;

    ASTTableJoin::Kind getKind() const { return kind; }
    ASTTableJoin::Strictness getStrictness() const { return strictness; }
    const std::optional<TypeIndex> & getAsofType() const { return asof_type; }
    ASOF::Inequality getAsofInequality() const { return asof_inequality; }
    bool anyTakeLastRow() const { return any_take_last_row; }

    const ColumnWithTypeAndName & rightAsofKeyColumn() const;

    /// proton : starts. Using types from HashJoin
    using Type = HashJoin::Type;
    using MapsOne = HashJoin::MapsOne;
    using MapsAll = HashJoin::MapsAll;
    using MapsAsof = HashJoin::MapsAsof;
    using MapsStreamingAsof = HashJoin::MapsStreamingAsof;
    using MapsVariant = HashJoin::MapsVariant;
    /// proton : ends

    using RawBlockPtr = const Block *;
    using BlockNullmapList = std::deque<std::pair<RawBlockPtr, ColumnPtr>>;

    struct StreamData
    {
        explicit StreamData(StreamingHashJoin * join_) : join(join_) { }

        StreamData(const RangeAsofJoinContext & range_asof_join_ctx_, const String & asof_column_name_, StreamingHashJoin * join_)
            : range_asof_join_ctx(range_asof_join_ctx_), asof_col_name(asof_column_name_), join(join_)
        {
            updateBucketSize();
        }

        void updateBucketSize()
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
            else if (
                range_asof_join_ctx.lower_bound
                == 0) /// we can't use upper_bound == 2 * bucket_size here for case like 0 < left - right < 13
            {
                /// case 5, [left_bucket - 2 * bucket_size, left_bucket]
                join_start_bucket = 2;
                join_stop_bucket = 0;
            }
            else
                assert(0);
        }

        template <typename V>
        size_t removeOldBuckets(std::map<Int64, V> & blocks, std::string_view stream)
        {
            Int64 watermark = join->combined_watermark;
            watermark -= bucket_size;

            size_t remaining_bytes = 0;

            std::vector<Int64> buckets_to_remove;
            {
                std::scoped_lock lock(mutex);

                for (auto iter = blocks.begin(); iter != blocks.end(); ++iter)
                {
                    if (iter->first <= watermark)
                    {
                        buckets_to_remove.push_back(iter->first);
                        iter = blocks.erase(iter);
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

        void updateAsofJoinColumnPositionAndScale(UInt16 scale, size_t asof_col_pos_, TypeIndex type_index)
        {
            range_asof_join_ctx.lower_bound *= intExp10(scale);
            range_asof_join_ctx.upper_bound *= intExp10(scale);
            asof_col_pos = asof_col_pos_;

            updateBucketSize();

            range_splitter = createBlockRangeSplitter(type_index, asof_col_pos, bucket_size, true);
        }

        void setHasNewData(bool has_new_data) { has_new_data_since_last_join = has_new_data; }
        bool hasNewData() const { return has_new_data_since_last_join; }

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

        struct StreamBlocks
        {
            explicit StreamBlocks(StreamData * stream_data_) : stream_data(stream_data_) { }

            ~StreamBlocks()
            {
                stream_data->metrics.current_total_blocks -= blocks.size();
                stream_data->metrics.current_total_bytes -= total_bytes;
                stream_data->metrics.total_blocks -= blocks.size();
                stream_data->metrics.total_bytes -= total_bytes;
                stream_data->metrics.gced_blocks += blocks.size();
            }

            void ALWAYS_INLINE updateMetrics(const Block & block)
            {
                min_ts = std::min(block.info.watermark_lower_bound, min_ts);
                max_ts = std::max(block.info.watermark, max_ts);

                /// Update metrics
                auto bytes = block.allocatedBytes();
                total_bytes += bytes;
                ++stream_data->metrics.current_total_blocks;
                stream_data->metrics.current_total_bytes += bytes;
                ++stream_data->metrics.total_blocks;
                stream_data->metrics.total_bytes += bytes;
            }

            void ALWAYS_INLINE negateMetrics(const Block & block)
            {
                /// Update metrics
                auto bytes = block.allocatedBytes();
                total_bytes -= bytes;
                --stream_data->metrics.current_total_blocks;
                stream_data->metrics.current_total_bytes -= bytes;
                --stream_data->metrics.total_blocks;
                stream_data->metrics.total_bytes -= bytes;
                ++stream_data->metrics.gced_blocks;
            }

            BlocksList blocks;
            Int64 min_ts = std::numeric_limits<Int64>::max();
            Int64 max_ts = -1;
            size_t total_bytes = 0;

            StreamData * stream_data;
        };

        bool has_new_data_since_last_join = false;
        RangeAsofJoinContext range_asof_join_ctx;
        Int64 bucket_size = 0;
        Int64 join_start_bucket = 0;
        Int64 join_stop_bucket = 0;
        String asof_col_name;
        Int64 asof_col_pos = -1;
        BlockRangeSplitterPtr range_splitter;
        std::atomic_int64_t current_watermark = 0;
        StreamingHashJoin * join;

        struct Metrics
        {
            size_t current_total_blocks = 0;
            size_t current_total_bytes = 0;
            size_t total_blocks = 0;
            size_t total_bytes = 0;
            size_t gced_blocks = 0;

            String string() const
            {
                return fmt::format(
                    "total_bytes={} total_blocks={} current_total_bytes={} current_total_blocks={} gced_blocks={}",
                    total_bytes,
                    total_blocks,
                    current_total_bytes,
                    current_total_blocks,
                    gced_blocks);
            }
        };
        Metrics metrics;

        std::mutex mutex;
    };

    struct RightTableData : public StreamData
    {
        using StreamData::StreamData;

        Type type = Type::EMPTY;

        Block sample_block; /// Block as it would appear in the BlockList

        struct RightTableBlocks : public StreamBlocks
        {
            using StreamBlocks::StreamBlocks;

            void insertBlock(Block && block)
            {
                /// FIXME, there are hashmap memory as well
                updateMetrics(block);

                blocks.push_back(std::move(block));
                has_new_data_since_last_join = true;
            }

            std::vector<MapsVariant> maps;
            BlockNullmapList blocks_nullmaps; /// Nullmaps for blocks of "right" table (if needed)

            bool has_new_data_since_last_join = false;

            /// Additional data - strings for string keys and continuation elements of single-linked lists of references to rows.
            Arena pool;

            bool hasNewData() const { return has_new_data_since_last_join; }
            void markNoNewData() { has_new_data_since_last_join = false; }
        };

        void insertBlock(
            Int64 bucket,
            Block block,
            ColumnRawPtrs & key_columns,
            JoinCommon::JoinMask & join_mask_col,
            ConstNullMapPtr null_map,
            UInt8 save_nullmap,
            ColumnPtr null_map_holder,
            ColumnUInt8::MutablePtr not_joined_map);

        size_t removeOldData() { return removeOldBuckets(hashed_blocks, "right"); }

        std::map<Int64, std::shared_ptr<RightTableBlocks>> hashed_blocks;

        /// `current_join_blocks` serves 3 purposes
        /// 1) During query plan phase, we will need it to evaluate the header
        /// 2) Workaround the `joinBlock` API interface for range join, it points the current working right blocks in the time bucket
        /// 3) For non-range join, it points the global blocks since there is no time bucket in this case
        std::shared_ptr<RightTableBlocks> current_join_blocks;
    };

    using RightTableDataPtr = std::shared_ptr<RightTableData>;

    /// proton : starts
    struct LeftTableData : public StreamData
    {
        using StreamData::StreamData;

        void insertBlock(Block && block);

        void insertBlockToTimeBucket(Block && block);

        size_t removeOldData() { return removeOldBuckets(blocks, "left"); }

        struct LeftTableBlocks : public StreamBlocks
        {
            explicit LeftTableBlocks(StreamData * stream_data_) : StreamBlocks(stream_data_), new_data_iter(blocks.end()) { }

            LeftTableBlocks(Block && block, StreamData * stream_data_) : StreamBlocks(stream_data_)
            {
                blocks.push_back(std::move(block));
                new_data_iter = blocks.begin();
                updateMetrics(*new_data_iter);
            }

            void insertBlock(Block && block)
            {
                updateMetrics(block);

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

            /// point to the new data node if there is, otherwise blocks.end()
            BlocksList::iterator new_data_iter;

            JoinTupleMap joined_rows;

            bool hasNewData() const { return new_data_iter != blocks.end(); }
            void markNoNewData() { new_data_iter = blocks.end(); }
        };
        using LeftTableBlocksPtr = std::shared_ptr<LeftTableBlocks>;

        UInt64 block_id = 0;
        std::map<Int64, LeftTableBlocksPtr> blocks;

        /// `current_join_blocks serves 2 different purpose
        /// 1) Workaround the `joinBlock` interface. For range join, it points to the current working blocks in the time bucket
        /// 2) For all join, it points to the global working blocks since there is not time bucket in this case
        LeftTableBlocksPtr current_join_blocks;
    };

    using LeftTableDataPtr = std::shared_ptr<LeftTableData>;
    /// proton : ends

    /// bool isUsed(size_t off) const { return used_flags.getUsedSafe(off); }
    /// bool isUsed(const Block * block_ptr, size_t row_idx) const { return used_flags.getUsedSafe(block_ptr, row_idx); }

    void setAsofJoinColumnPositionAndScale(UInt16 scale, size_t left_asof_col_pos, size_t right_asof_col_pos, TypeIndex type_index)
    {
        left_data->updateAsofJoinColumnPositionAndScale(scale, left_asof_col_pos, type_index);
        right_data->updateAsofJoinColumnPositionAndScale(scale, right_asof_col_pos, type_index);
    }

private:
    void dataMapInit(MapsVariant &);

    const Block & savedBlockSample() const { return right_data->sample_block; }

    /// Modify (structure) right block to save it in block list
    Block structureRightBlock(const Block & stored_block) const;
    void initRightBlockStructure(Block & saved_block_sample);

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
    void joinBlockImpl(Block & block, const Block & block_with_columns_to_add, const std::vector<const Maps *> & maps_) const;

    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);

    bool empty() const;

    /// proton : starts
    void initData();

    void initHashMaps(std::vector<MapsVariant> & all_maps);

    /// When left stream joins right stream, watermark calculation is more complicated.
    /// Firstly, each stream has its own watermark and progresses separately.
    /// Secondly, `combined_watermark` is calculated periodically according the watermarks in the left and right streams
    void calculateWatermark();

    std::pair<size_t, size_t> removeOldData();

    /// For global join without time bucket
    Block joinBlocksAll(size_t & left_cached_bytes, size_t & right_cached_bytes);
    void doJoinBlock(Block & block);
    /// proton : ends

private:
    template <bool>
    friend class NotJoinedHash;

    friend class JoinSource;

    std::shared_ptr<TableJoin> table_join;
    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strictness;

    bool nullable_right_side; /// In case of LEFT and FULL joins, if use_nulls, convert right-side columns to Nullable.
    bool nullable_left_side; /// In case of RIGHT and FULL joins, if use_nulls, convert left-side columns to Nullable.
    bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    std::optional<TypeIndex> asof_type;
    ASOF::Inequality asof_inequality;

    JoinStreamDescription left_stream_desc;
    JoinStreamDescription right_stream_desc;

    LeftTableDataPtr left_data;

    /// Right table data. StorageJoin shares it between many Join objects.
    /// Flags that indicate that particular row already used in join.
    /// Flag is stored for every record in hash map.
    /// Number of this flags equals to hashtable buffer size (plus one for zero value).
    /// Changes in hash table broke correspondence,
    /// so we must guarantee constantness of hash table during HashJoin lifetime (using method setLock)
    /// mutable JoinStuff::JoinUsedFlags used_flags;
    RightTableDataPtr right_data;

    std::vector<Sizes> key_sizes;

    /// Block with columns from the right-side table except key columns.
    Block sample_block_with_columns_to_add;
    /// Block with key columns in the same order they appear in the right-side table (duplicates appear once).
    Block right_table_keys;
    /// Block with key columns right-side table keys that are needed in result (would be attached after joined columns).
    Block required_right_keys;
    /// Left table column names that are sources for required_right_keys columns
    std::vector<String> required_right_keys_sources;

    Block totals;

    std::atomic_int64_t combined_watermark = 0;

    struct JoinMetrics
    {
        size_t total_join = 0;
        size_t no_new_data_skip = 0;
        size_t time_bucket_no_new_data_skip = 0;
        size_t time_bucket_no_intersection_skip = 0;
        size_t left_block_and_right_time_bucket_no_intersection_skip = 0;
        size_t only_join_new_data = 0;

        String string() const
        {
            return fmt::format(
                "total_join={} no_new_data_skip={} time_bucket_no_new_data_skip={} time_bucket_no_intersection_skip={}  "
                "left_block_and_right_time_bucket_no_intersection_skip={} only_join_new_data={}",
                total_join,
                no_new_data_skip,
                time_bucket_no_new_data_skip,
                time_bucket_no_intersection_skip,
                left_block_and_right_time_bucket_no_intersection_skip,
                only_join_new_data);
        }
    };

    JoinMetrics join_metrics;

    Poco::Logger * log;
};

}
