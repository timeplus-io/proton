#pragma once

#include "JoinStreamDescription.h"
#include "RowRefs.h"
#include "joinData.h"
#include "joinKind.h"

#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/ColumnUtils.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace Streaming
{
/// Streaming hash join: there are several join semantic
/// 1) append-only `global join` append-only: SELECT * FROM left_stream INNER JOIN right_stream ON left_stream.key = right_stream.key;
///    i. Continuously reading data from left stream and cache all of the data in-memory. Keep tracking `the un-joined new data`
///    ii. Continuously reading data from right stream and build hashtable for right stream incrementally and continuously. Keep tracing `the un-joined new data`
///    iii. Join the (new) data only periodically
///    (LEFT_DATA + LEFT_NEW_DATA) x (RIGHT_DATA + RIGHT_NEW_DATA) => we only join the new data, so the every time it joins, it emits
///    LEFT_NEW_DATA x (RIGHT_DATA + RIGHT_NEW_DATA) +  RIGHT_NEW_DATA x (LEFT_DATA + LEFT_NEW_DATA)
///
///   Note, we don't recycle any data for global streaming join for now. So data will keep growing until hit a limit and the nwe abort
///   We treat global stream join as experimental only for now.
///
/// 2) append-only `interval join` append-only: SELECT * FROM left_stream INNER JOIN right_stream ON left_stream.key = right_stream.key ON date_diff_within(10s);
///    i. Continuously reading data from left stream, build time buckets and put data into appropriate buckets. Keep tracking `the un-joined new data / bucket` and its current timestamp.
///    ii. Continuously reading data from right stream, build time buckets and put data into appropriate buckets. Keep tracking `the un-joined new data / bucket` and its current timestamp.
///        In the meanwhile, build hashtable for the data in each bucket
///    iii. Join the (new) data only according to time bucket periodically and progress the timestamp as `current_timestamp = std::min(current_left_timestamp, current_right_timestamp)`;
///    IV. Prune expired buckets and their associated data.
///
///    For example, current timestamp progresses to bucket-3, when there are new data in left_bucket-3 and there are 3 corresponding buckets on right stream as well. The join will look like below
///    [left_bucket-1, left_bucket-2, left_bucket-3 with new data] x [right_bucket-1, right_bucket-2, right_bucket-3] =>
///    left_bucket-3 x [right-bucket-2, right-bucket-3]
///    After the join, prune `left_bucket-1, right_bucket-1` since we assume there will no new data flows into these buckets`. We can't prune `right-bucket-2` because
///    current timestamp is still in bucket-3, so there may be some new data flows into `left_bucket-3` which can join `right_bucket-2`.
///    Similarly, we can't prune `left_bucket-2` as there may be some new data flowing into `right_bucket-3` which can join `left_bucket-2`.
///
///    Note, Garbage collection depends on correct timestamp progressing for `interval join`. Old data will be discarded according to the current progressing timestamp.
///    So progressing timestamp is something very important and challenging.
///
/// 3) append-only `global join` versioned-kv: SELECT * FROM left_stream INNER JOIN right_versioned_kv_stream ON left_stream.key = right_versioned_kv_stream.key;
///    i. Merge reading all historical data for versioned_kv and build hashtable for joined key. Merge reading means during query time, it will keeps max last X version of per key
///       If a key has more than X versions, any older version will be discarded during merge reading. If a key doesn't have X versions, all versions will be kept.
///       By default, only the latest version will be used.
///    ii. After reading all historical data, continuously reading "new" data from right stream and in the meanwhile, update the hashtable to keep the last X versions per key.
///       So there may be some garbage collection happening if a key has more than X versions.
///    iii. Continuously reading data from left stream and in the meanwhile continuously join the left data with the right side hashtable with the closed version.
///       Please note, we  don't buffer left stream data in-memory at all. So if a row in left stream can't find a match in the right hashtable,
///       we don't hold onto this row in-memory for future possible join.
///
///  4) versioned-kv `global join` versioned-kv: SELECT * FROM left_versioned_kv JOIN right_versioned_kv ON left_versioned_kv.key = right_versioned_kv.key;
///     i. Merge reading all historical data from left stream and build left hashtable for joined key. In parallel, merge reading all historical data from right stream and build right hashtable.
///        Keep tracking which part is new (un-joined) data for left stream and right stream.
///     ii. Continuously reading "new" data from left stream and update left hashtable. In parallel, continuously reading "new" data from right stream and update hashtable.
///        Keep tracking which part is new (un-joined) data for left stream and right stream.
///     iv. Periodically, join the "new" left data with the right hashtable and join the "new" right data with the "left" hashtable. Emit the joined rows and do further "changelog" processing
///        a. Loop all joined rows, lookup the new joined rows in the previous buffered joined rows by using joining key.
///        b. If a new joined row doesn't exist in previous "buffered" joined rows, buffer it and also final emit it to down stream. (This is a new row)
///        c. If a new joined row does exist in previous "buffered" joined rows, then
///           i. Emit the buffered joined rows with negate (delta_flag = -1) if downstream processor needs this semantic. (We need subtract it (from an a global "count" or "sum" for example))
///           ii. Emit the new joined rows as well.
///           iii. Replace the buffered joined rows with the new joined row.
///
///     Note, this is a very special join with very special changelog semantic.
///
///  5) append-only `global join` versioned-kv: SELECT * FROM left_stream ASOF JOIN right_versioned_kv_stream ON left_stream.key = right_versioned_kv_stream.key AND left.timestamp < right.timestamp;
///

struct HashJoinMapsVariants;

class HashJoin final : public IJoin
{
public:
    HashJoin(
        std::shared_ptr<TableJoin> table_join_,
        JoinStreamDescription left_join_stream_desc_,
        JoinStreamDescription right_join_stream_desc_);

    ~HashJoin() noexcept override;

    /// When left stream header is known, init data structure in hash join for left stream
    void initLeftStream(const Block & left_header);

    /// returns total bytes in left stream cache
    size_t insertLeftBlock(Block left_input_block);

    /// returns total bytes in right stream cache
    size_t insertRightBlock(Block right_input_block);

    Block joinBlocks(size_t & left_cached_bytes, size_t & right_cached_bytes);

    /// For bidirectional join
    /// Return retracted block
    Block joinWithRightBlocks(Block & left_block);
    Block joinWithLeftBlocks(Block & right_block, const Block & output_header);

    bool needBufferLeftStream() const { return right_stream_desc.data_stream_semantic != DataStreamSemantic::VersionedKV; }

    /// If append-only join append-only, versioned-kv join versioned-kv or changelog-kv join changelog-kv,
    /// we do bi-directional hash join, so build hashtable for left stream
    bool emitChangeLog() const
    {
        return (left_stream_desc.data_stream_semantic == DataStreamSemantic::VersionedKV
                && left_stream_desc.data_stream_semantic == DataStreamSemantic::VersionedKV)
            || (left_stream_desc.data_stream_semantic == DataStreamSemantic::ChangeLogKV
                && right_stream_desc.data_stream_semantic == DataStreamSemantic::ChangeLogKV);
    }

    bool bidirectionalHashJoin() const
    {
        /// For any join, we don't need buffer left stream data, right stream is acting like
        /// a dynamic dict with versioned key for real-time enrichment.
        /// For this case, we only build hashtable for right stream data
        return streaming_strictness != Strictness::Any;
    }

    UInt64 keepVersions() const { return right_stream_desc.keep_versions; }

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

    JoinKind getKind() const { return kind; }
    JoinStrictness getStrictness() const { return strictness; }
    Strictness getStreamingStrictness() const { return streaming_strictness; }
    const std::optional<TypeIndex> & getAsofType() const { return asof_type; }
    ASOFJoinInequality getAsofInequality() const { return asof_inequality; }
    bool anyTakeLastRow() const { return any_take_last_row; }

    const ColumnWithTypeAndName & rightAsofKeyColumn() const;

/// Different types of keys for maps.
#define APPLY_FOR_JOIN_VARIANTS(M) \
    M(key8) \
    M(key16) \
    M(key32) \
    M(key64) \
    M(key_string) \
    M(key_fixed_string) \
    M(keys128) \
    M(keys256) \
    M(hashed)


/// Used for reading from StorageJoin and applying joinGet function
#define APPLY_FOR_JOIN_VARIANTS_LIMITED(M) \
    M(key8) \
    M(key16) \
    M(key32) \
    M(key64) \
    M(key_string) \
    M(key_fixed_string)

    enum class Type
    {
        EMPTY,
        CROSS,
        DICT,
#define M(NAME) NAME,
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    };

    /** Different data structures, that are used to perform JOIN.
      */
    template <typename Mapped>
    struct MapsTemplate
    {
        using MappedType = Mapped;
        std::unique_ptr<FixedHashMap<UInt8, Mapped>> key8;
        std::unique_ptr<FixedHashMap<UInt16, Mapped>> key16;
        std::unique_ptr<HashMap<UInt32, Mapped, HashCRC32<UInt32>>> key32;
        std::unique_ptr<HashMap<UInt64, Mapped, HashCRC32<UInt64>>> key64;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>> key_string;
        std::unique_ptr<HashMapWithSavedHash<StringRef, Mapped>> key_fixed_string;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128HashCRC32>> keys128;
        std::unique_ptr<HashMap<UInt256, Mapped, UInt256HashCRC32>> keys256;
        std::unique_ptr<HashMap<UInt128, Mapped, UInt128TrivialHash>> hashed;

        void create(Type which)
        {
            switch (which)
            {
                case Type::EMPTY:
                    break;
                case Type::CROSS:
                    break;
                case Type::DICT:
                    break;

#define M(NAME) \
    case Type::NAME: \
        NAME = std::make_unique<typename decltype(NAME)::element_type>(); \
        break;
                    APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }
        }

        size_t getTotalRowCount(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:
                    return 0;
                case Type::CROSS:
                    return 0;
                case Type::DICT:
                    return 0;

#define M(NAME) \
    case Type::NAME: \
        return NAME ? NAME->size() : 0;
                    APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }

            UNREACHABLE();
        }

        size_t getTotalByteCountImpl(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:
                    return 0;
                case Type::CROSS:
                    return 0;
                case Type::DICT:
                    return 0;

#define M(NAME) \
    case Type::NAME: \
        return NAME ? NAME->getBufferSizeInBytes() : 0;
                    APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }

            UNREACHABLE();
        }

        size_t getBufferSizeInCells(Type which) const
        {
            switch (which)
            {
                case Type::EMPTY:
                    return 0;
                case Type::CROSS:
                    return 0;
                case Type::DICT:
                    return 0;

#define M(NAME) \
    case Type::NAME: \
        return NAME ? NAME->getBufferSizeInCells() : 0;
                    APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }

            UNREACHABLE();
        }
    };

    using MapsOne = MapsTemplate<RowRefWithRefCount>;
    using MapsAll = MapsTemplate<RowRefList>;
    using MapsAsof = MapsTemplate<AsofRowRefs>;
    using MapsRangeAsof = MapsTemplate<RangeAsofRowRefs>;
    using MapsVariant = std::variant<MapsOne, MapsAll, MapsAsof, MapsRangeAsof>;

    /// bool isUsed(size_t off) const { return used_flags.getUsedSafe(off); }
    /// bool isUsed(const Block * block_ptr, size_t row_idx) const { return used_flags.getUsedSafe(block_ptr, row_idx); }

    void setAsofJoinColumnPositionAndScale(UInt16 scale, size_t left_asof_col_pos, size_t right_asof_col_pos, TypeIndex type_index)
    {
        left_data.buffered_data->updateAsofJoinColumnPositionAndScale(scale, left_asof_col_pos, type_index);
        right_data.buffered_data->updateAsofJoinColumnPositionAndScale(scale, right_asof_col_pos, type_index);
    }

    friend struct BufferedStreamData;

    /// For changelog emit
    struct JoinResults
    {
        JoinResults() : blocks(metrics), maps(std::make_unique<HashJoinMapsVariants>()) { }

        void addBlockWithoutLock(Block && block)
        {
            block.info.setBlockID(block_id++);
            blocks.updateMetrics(block);
        }

        std::mutex mutex;

        JoinMetrics metrics;
        JoinBlockList blocks;

        UInt64 block_id = 0;

        /// Arena pool to hold the (string) keys
        Arena pool;

        /// Building hash map for joined blocks, then we can find previous
        /// join blocks quickly by using joined keys
        std::unique_ptr<HashJoinMapsVariants> maps;
    };

private:
    void dataMapInit(MapsVariant &);

    const Block & savedRightBlockSample() const { return right_data.buffered_data->sample_block; }
    const Block & savedLeftBlockSample() const { return left_data.buffered_data->sample_block; }

    /// Modify right block (update structure according to sample block) to save it in block list
    Block prepareRightBlock(const Block & block) const;
    Block prepareLeftBlock(const Block & block) const;

    /// Remove columns which are not needed to be projected
    static Block prepareBlockToSave(const Block & block, const Block & sample_block);

    void initRightBlockStructure();
    void initLeftBlockStructure();
    void initBlockStructure(Block & saved_block_sample, const Block & table_keys, const Block & sample_block_with_columns_to_add) const;

    template <Kind KIND, Strictness STRICTNESS, typename Maps>
    void joinBlockImpl(Block & left_block, const Block & block_with_columns_to_add, const std::vector<const Maps *> & maps_) const;

    template <Kind KIND, Strictness STRICTNESS, typename Maps>
    void joinBlockImplLeft(Block & right_block, const Block & block_with_columns_to_add, const std::vector<const Maps *> & maps_) const;

    void chooseHashMethod();
    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);

    bool empty() const;

    void initBufferedData();

    void initHashMaps(std::vector<MapsVariant> & all_maps);

    /// When left stream joins right stream, watermark calculation is more complicated.
    /// Firstly, each stream has its own watermark and progresses separately.
    /// Secondly, `combined_watermark` is calculated periodically according the watermarks in the left and right streams
    void calculateWatermark();

    /// For global join without time bucket
    Block joinBlocksAll(size_t & left_cached_bytes, size_t & right_cached_bytes);
    Block joinBlocksBidirectional(size_t & left_cached_bytes, size_t & right_cached_bytes);
    void doJoinBlock(Block & block);
    void checkJoinSemantic() const;

    void addLeftHashBlock(Block source_block);
    void addRightHashBlock(Block source_block);

    /// `retract` does
    /// 1) Add result_block to JoinResults
    /// 2) Retract previous joined block. changelog emit
    Block retract(const Block & result_block);


private:
    template <bool>
    friend class NotJoinedHash;

    std::shared_ptr<TableJoin> table_join;
    JoinKind kind;
    JoinStrictness strictness;

    Kind streaming_kind;
    Strictness streaming_strictness;

    bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    std::optional<TypeIndex> asof_type;
    ASOFJoinInequality asof_inequality;

    std::vector<Sizes> key_sizes;

    /// Left stream related
    JoinStreamDescription left_stream_desc;
    /// Right stream related
    JoinStreamDescription right_stream_desc;

    struct JoinData
    {
        BufferedStreamDataPtr buffered_data;

        /// Block with columns from the (left or) right-side table except key columns.
        Block sample_block_with_columns_to_add;
        /// Block with key columns in the same order they appear in the (left or) right-side table (duplicates appear once).
        Block table_keys;
        /// Block with key columns (left or) right-side table keys that are needed in result (would be attached after joined columns).
        Block required_keys;
        /// Left table column names that are sources for required_right_keys columns
        std::vector<String> required_keys_sources;
    };

    /// Note: when left block joins right hashtable, use `right_data`
    JoinData right_data;
    /// Note: when right block joins left hashtable, use `left_data`
    JoinData left_data;

    /// Right table data. StorageJoin shares it between many Join objects.
    /// Flags that indicate that particular row already used in join.
    /// Flag is stored for every record in hash map.
    /// Number of this flags equals to hashtable buffer size (plus one for zero value).
    /// Changes in hash table broke correspondence,
    /// mutable JoinStuff::JoinUsedFlags used_flags;

    Type hash_method_type;

    std::optional<JoinResults> join_results;

    Block totals;

    std::atomic_int64_t combined_watermark = 0;

    JoinGlobalMetrics join_metrics;

    Poco::Logger * log;
};

struct HashJoinMapsVariants
{
    std::vector<HashJoin::MapsVariant> map_variants;
};

}
}
