#pragma once

#include <Interpreters/Streaming/IHashJoin.h>
#include <Interpreters/Streaming/JoinStreamDescription.h>
#include <Interpreters/Streaming/RowRefs.h>
#include <Interpreters/Streaming/joinData.h>
#include <Interpreters/Streaming/joinKind.h>

#include <Interpreters/AggregationCommon.h>
#include <Interpreters/TableJoin.h>
#include <Common/ColumnUtils.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ProtonCommon.h>
#include <base/SerdeTag.h>

namespace DB
{
namespace Streaming
{
/// Streaming hash join: there are several join semantic
/// 1) append-only `INNER JOIN` append-only: SELECT * FROM left_stream INNER JOIN right_stream ON left_stream.key = right_stream.key;
///    i. Continuously reading data from left stream, cache all of the data in-memory and build hashtable for left stream continuously. Join new data from left stream with right hash table.
///    ii. Continuously reading data from right stream, cache all of the data in-memory and build hashtable for right stream continuously. Join new data from right stream with left hash table.
///
///   Note, we don't recycle any data for global streaming join for now. So data will keep growing until hit a limit and the nwe abort
///   We treat global stream join as experimental only for now.
///
/// 2) append-only `INNER RANGE JOIN` append-only: SELECT * FROM left_stream INNER JOIN right_stream ON left_stream.key = right_stream.key ON date_diff_within(10s);
///    i. Continuously reading data from left stream, build time buckets, put data into appropriate buckets and build hashtable for each bucket. Range join new data from left stream with the corresponding right bucket hash tables. Progress the timestamp.
///    ii. Continuously reading data from right stream, build time buckets and put data into appropriate buckets and build hashtable for each bucket. Range join new data from right stream with the corresponding left bucket hash tables. Progress the timestamp.
///    iii. Prune expired buckets and their associated data.
///
///    For example, current timestamp progresses to bucket-3, when there are new data in left_bucket-3 and there are 3 corresponding buckets on right stream as well. The join will look like below
///    [left_bucket-1, left_bucket-2, left_bucket-3 with new data] x [right_bucket-1, right_bucket-2, right_bucket-3] =>
///    left_bucket-3 x [right-bucket-2, right-bucket-3]
///    After the join, prune `left_bucket-1, right_bucket-1` since we assume there will no new data flows into these buckets`. We can't prune `right-bucket-2` because
///    current timestamp is still in bucket-3, so there may be some new data flows into `left_bucket-3` which can join `right_bucket-2`.
///    Similarly, we can't prune `left_bucket-2` as there may be some new data flowing into `right_bucket-3` which can join `left_bucket-2`.
///
///    Note, Garbage collection depends on correct timestamp progression for `interval join`. Old data will be discarded according to the current progressing timestamp.
///    So progressing timestamp is something very important and challenging.
///
/// 3) append-only `INNER JOIN` versioned-kv: SELECT * FROM left_stream INNER JOIN right_versioned_kv_stream ON left_stream.key = right_versioned_kv_stream.key;
///    i. Merge reading all historical data for versioned_kv and build hashtable for joined key. Merge reading means during query time, it will keeps only the last version of per key
///    ii. After reading all historical data, continuously reading "new" data from right stream and in the meanwhile, update the hashtable and keep the last versions per key.
///       So there may be some garbage collection happening.
///    iii. Continuously reading data from left stream and in the meanwhile continuously join the left data with the right side hashtable.
///
///    Please note, we  don't buffer left stream data in-memory at all. So if a row in left stream can't find a match in the right hashtable,
///    we don't hold onto this row in-memory for future possible join. This join acts like a dynamic data enrichment
///
/// 4) append-only `INNER ASOF JOIN` versioned-kv: SELECT * FROM left_stream INNER ASOF JOIN right_versioned_kv_stream ON left_stream.key = right_versioned_kv_stream.key;
///    i. Merge reading all historical data for versioned_kv and build hashtable for joined key. Merge reading means during query time, it will keeps last X version of per key
///       By default, it keeps 3 versions per key.
///    ii. After reading all historical data, continuously reading "new" data from right stream and in the meanwhile, update the hashtable and keep the last X versions per key.
///       So there may be some garbage collection happening.
///    iii. Continuously reading data from left stream and in the meanwhile continuously join the left data with the right side hashtable on join keys and on closest asof join keys.
///
///    Please note, we  don't buffer left stream data in-memory at all. So if a row in left stream can't find a match in the right hashtable,
///    we don't hold onto this row in-memory for future possible join. This join acts like a dynamic data enrichment
///
///  5) append-only `INNER ASOF JOIN` append-only : SELECT * FROM left_stream INNER ASOF JOIN right_stream ON left_stream.key = right_stream.key AND left_stream._tp_time < right_stream._tp_time;
///     i. Continuously reading data from right stream, build hash table on joined keys and also saves latest X versions of per key
///       By default, it keeps 3 versions per key. So there may be some garbage collection happening.
///     ii. Continuously reading data from left stream and in the meanwhile continuously join the left data with the right side hashtable on join keys and on closest asof join keys.
///
///    Please note, we  don't buffer left stream data in-memory at all. So if a row in left stream can't find a match in the right hashtable,
///    we don't hold onto this row in-memory for future possible join. This join acts like a dynamic data enrichment
///
///  6) versioned-kv `INNER JOIN` versioned-kv: SELECT * FROM left_versioned_kv JOIN right_versioned_kv ON left_versioned_kv.key = right_versioned_kv.key;
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
///  7) append-only `INNER LATEST JOIN` append-only: SELECT * FROM left_stream INNER LATEST JOIN right_stream ON left_stream.key = right_stream.key;
///    i. Continuously reading data from left stream, build hashtable for right stream continuously and only keep the latest key / value
///    ii. Continuously reading data from left stream and in the meanwhile continuously join the left data with the right side hashtable.

struct HashJoinMapsVariants;

class HashJoin final : public IHashJoin
{
public:
    using SupportMatrix = std::unordered_map<
        DataStreamSemantic,
        std::unordered_map<JoinKind, std::unordered_map<JoinStrictness, std::unordered_map<DataStreamSemantic, bool>>>>;

    /// So far we only support these join combinations
    static const SupportMatrix support_matrix;

public:
    HashJoin(
        std::shared_ptr<TableJoin> table_join_,
        JoinStreamDescriptionPtr left_join_stream_desc_,
        JoinStreamDescriptionPtr right_join_stream_desc_);

    ~HashJoin() noexcept override;

    /// Do post initialization
    /// When left stream header is known, init data structure in hash join for left stream
    void postInit(const Block & left_header, const Block & output_header_, UInt64 join_max_cached_bytes_) override;

    void transformHeader(Block & header) override;

    /// For non-bidirectional hash join
    void insertRightBlock(Block right_block) override;
    void joinLeftBlock(Block & left_block) override;

    /// For bidirectional hash join
    /// There are 2 blocks returned : joined block via parameter and retracted block via returned-value if there is
    Block insertLeftBlockAndJoin(Block & left_block) override;
    Block insertRightBlockAndJoin(Block & right_block) override;

    /// For bidirectional range hash join, there may be multiple joined blocks
    std::vector<Block> insertLeftBlockToRangeBucketsAndJoin(Block left_block) override;
    std::vector<Block> insertRightBlockToRangeBucketsAndJoin(Block right_block) override;

    /// "Legacy API", use insertRightBlock()
    bool addJoinedBlock(const Block & block, bool check_limits) override;

    /// "Legacy API", use joinLeftBlock()
    void joinBlock(Block & block, ExtraBlockPtr & not_processed) override;

    bool emitChangeLog() const override { return emit_changelog; }
    bool bidirectionalHashJoin() const override { return bidirectional_hash_join; }
    bool rangeBidirectionalHashJoin() const override { return range_bidirectional_hash_join; }

    UInt64 keepVersions() const { return right_data.join_stream_desc->keep_versions; }

    const TableJoin & getTableJoin() const override { return *table_join; }

    void checkTypesOfKeys(const Block & block) const override;

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

    void getKeyColumnPositions(
        std::vector<size_t> & left_key_column_positions,
        std::vector<size_t> & right_key_column_positions,
        bool include_asof_key_column) const override
    {
        auto calc_key_positions = [](const auto & key_column_names, const auto & header, auto & key_column_positions) {
            key_column_positions.reserve(key_column_names.size());
            for (const auto & name : key_column_names)
                key_column_positions.push_back(header.getPositionByName(name));
        };

        calc_key_positions(table_join->getOnlyClause().key_names_left, left_data.join_stream_desc->input_header, left_key_column_positions);
        calc_key_positions(
            table_join->getOnlyClause().key_names_right, right_data.join_stream_desc->input_header, right_key_column_positions);

        if (!include_asof_key_column && (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof))
        {
            left_key_column_positions.pop_back();
            right_key_column_positions.pop_back();
        }

        assert(!left_key_column_positions.empty());
        assert(!right_key_column_positions.empty());
    }

    JoinKind getKind() const { return kind; }
    JoinStrictness getStrictness() const { return strictness; }
    Kind getStreamingKind() const { return streaming_kind; }
    Strictness getStreamingStrictness() const { return streaming_strictness; }
    const std::optional<TypeIndex> & getAsofType() const { return asof_type; }
    ASOFJoinInequality getAsofInequality() const { return asof_inequality; }
    bool anyTakeLastRow() const { return any_take_last_row; }

    const ColumnWithTypeAndName & rightAsofKeyColumn() const { return right_data.asof_key_column; }
    const ColumnWithTypeAndName & leftAsofKeyColumn() const { return left_data.asof_key_column; }

    const Block & getOutputHeader() const { return output_header; }

    void serialize(WriteBuffer & wb) const override;
    void deserialize(ReadBuffer & rb) override;

    void cancel() override { }

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
#define M(NAME) \
    case Type::NAME: \
        return NAME ? NAME->getBufferSizeInCells() : 0;
                APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }

            UNREACHABLE();
        }
    };

    using MapsOne = MapsTemplate<RowRefWithRefCount<Block>>;
    using MapsMultiple = MapsTemplate<RowRefListMultiplePtr>;
    using MapsAll = MapsTemplate<RowRefList>;
    using MapsAsof = MapsTemplate<AsofRowRefs>;
    using MapsRangeAsof = MapsTemplate<RangeAsofRowRefs>;
    using MapsVariant = std::variant<MapsOne, MapsAll, MapsAsof, MapsRangeAsof, MapsMultiple>;

    size_t sizeOfMapsVariant(const MapsVariant & maps_variant) const;
    Type getHashMethodType() const { return hash_method_type; }

    /// bool isUsed(size_t off) const { return used_flags.getUsedSafe(off); }
    /// bool isUsed(const Block * block_ptr, size_t row_idx) const { return used_flags.getUsedSafe(block_ptr, row_idx); }

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

        String joinMetricsString(const HashJoin * join) const;

        mutable std::mutex mutex;

        CachedBlockMetrics metrics;
        RefCountBlockList<Block> blocks;

        UInt64 block_id = 0;

        /// Arena pool to hold the (string) keys
        Arena pool;

        /// Building hash map for joined blocks, then we can find previous
        /// join blocks quickly by using joined keys
        std::unique_ptr<HashJoinMapsVariants> maps;
    };

private:
    void checkJoinSemantic() const;
    void init();
    void initBufferedData();
    void initHashMaps(std::vector<MapsVariant> & all_maps);
    void dataMapInit(MapsVariant &);

    /// For versioned kv / changelog kv
    void initLeftPrimaryKeyHashTable();
    void initRightPrimaryKeyHashTable();
    void reviseJoinStrictness();

    void initLeftBlockStructure();
    void initRightBlockStructure();
    struct JoinData;
    void initBlockStructure(JoinData & join_data, const Block & table_keys, const Block & sample_block_with_columns_to_add) const;

    void chooseHashMethod();
    static Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);

    void validateAsofJoinKey();

    void checkLimits() const;

    const Block & savedLeftBlockSample() const { return left_data.buffered_data->sample_block; }
    const Block & savedRightBlockSample() const { return right_data.buffered_data->sample_block; }

    /// Modify left or right block (update structure according to sample block) to save it in block list
    template <bool is_left_block>
    Block prepareBlock(const Block & block) const;

    /// Remove columns which are not needed to be projected
    static Block prepareBlockToSave(const Block & block, const Block & sample_block);

    /// For range bidirectional hash join
    template <bool is_left_block>
    std::vector<Block> insertBlockToRangeBucketsAndJoin(Block block);

    template <bool is_left_block>
    Int64 doInsertBlock(Block block, HashBlocksPtr target_hash_blocks, std::vector<RowRefListMultipleRef *> row_refs = {});

    /// For bidirectional hash join
    /// Return retracted block if needs emit changelog, otherwise empty block
    template <bool is_left_block>
    Block joinBlockWithHashTable(Block & block, HashBlocksPtr target_hash_blocks);

    template <bool is_left_block>
    void doJoinBlockWithHashTable(Block & block, HashBlocksPtr target_hash_blocks);

    /// Join left block with right hash table or join right block with left hash table
    template <bool is_left_block, Kind KIND, Strictness STRICTNESS, typename Maps>
    void joinBlockImpl(Block & block, const Block & block_with_columns_to_add, const std::vector<const Maps *> & maps_) const;

    /// `retract` does
    /// 1) Add result_block to JoinResults
    /// 2) Retract previous joined block. changelog emit
    Block retract(const Block & result_block);

    /// When left stream joins right stream, watermark calculation is more complicated.
    /// Firstly, each stream has its own watermark and progresses separately.
    /// Secondly, `combined_watermark` is calculated periodically according the watermarks in the left and right streams
    void calculateWatermark();

    /// For versioned-kv / changelog-kv join
    /// Check if this ia row with a new primary key or an existing primary key
    /// For the later, erase the element in the target join linked list
    std::vector<RowRefListMultipleRef *> eraseOrAppendForPartialPrimaryKeyJoin(const Block & block);

    template <typename KeyGetter, typename Map>
    std::vector<RowRefListMultipleRef *> eraseOrAppendForPartialPrimaryKeyJoin(Map & map, ColumnRawPtrs && primary_key_columns);

    /// If the left / right_block is a retraction block : rows in `_tp_delta` column all have `-1`
    /// We erase the previous key / values from hash table
    /// Use for append JOIN changelog_kv / versioned_kv case
    /// @return true if keys are erased, otherwise false
    template <bool is_left_block>
    void eraseExistingKeys(Block & block, JoinData & join_data);
    inline bool isRetractBlock(const Block & block, const JoinStreamDescription & join_stream_desc);

    template <typename KeyGetter, typename Map>
    void doEraseExistingKeys(
        Map & map,
        const DB::Block & right_block,
        ColumnRawPtrs && key_columns,
        const Sizes & key_size,
        const std::vector<size_t> & skip_columns,
        bool delete_key);

    /// For bidirectional join, the input is a retract block
    template <bool is_left_block>
    std::optional<Block> eraseExistingKeysAndRetractJoin(Block & left_block);

private:
    /// Only SERDE the clauses of join
    SERDE std::shared_ptr<TableJoin> table_join;
    JoinKind kind;
    JoinStrictness strictness;

    SERDE Kind streaming_kind;
    SERDE Strictness streaming_strictness;

    bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    /// Only SERDE for ASOF JOIN or RANGE JOIN
    SERDE std::optional<TypeIndex> asof_type;
    SERDE ASOFJoinInequality asof_inequality;

    /// Cache data members which avoid re-computation for every join
    std::vector<std::pair<String, String>> cond_column_names;

    SERDE std::vector<Sizes> key_sizes;

    /// versioned-kv and changelog-kv both needs define a primary key
    /// If join on partial primary key columns (or no primary key column is expressed in join on clause),
    /// init this data structure
    struct PrimaryKeyHashTable
    {
        PrimaryKeyHashTable(Type hash_method_type_, Sizes && key_size_);

        Type hash_method_type;

        Sizes key_size;

        /// For key allocation
        Arena pool;

        /// Hash maps variants indexed by primary key columns
        MapsTemplate<RowRefListMultipleRefPtr> map;
    };
    using PrimaryKeyHashTablePtr = std::shared_ptr<PrimaryKeyHashTable>;

    struct JoinData
    {
        explicit JoinData(JoinStreamDescriptionPtr join_stream_desc_) : join_stream_desc(std::move(join_stream_desc_))
        {
            assert(join_stream_desc);
        }

        JoinStreamDescriptionPtr join_stream_desc;

        ColumnWithTypeAndName asof_key_column;

        /// Only for kv join. Used to maintain all unique keys
        PrimaryKeyHashTablePtr primary_key_hash_table;

        BufferedStreamDataPtr buffered_data;

        /// Block with columns from the (left or) right-side table except key columns.
        Block sample_block_with_columns_to_add;
        /// Block with key columns in the same order they appear in the (left or) right-side table (duplicates appear once).
        Block table_keys;
        /// Block with key columns (left or) right-side table keys that are needed in result (would be attached after joined columns).
        Block required_keys;
        /// Left table column names that are sources for required_right_keys columns
        std::vector<String> required_keys_sources;

        bool validated_join_key_types = false;
    };

    friend void serialize(const JoinData & join_data, WriteBuffer & wb);
    friend void deserialize(JoinData & join_data, ReadBuffer & rb);

    /// Note: when left block joins right hashtable, use `right_data`
    SERDE JoinData right_data;
    /// Note: when right block joins left hashtable, use `left_data`
    SERDE JoinData left_data;

    /// Right table data. StorageJoin shares it between many Join objects.
    /// Flags that indicate that particular row already used in join.
    /// Flag is stored for every record in hash map.
    /// Number of this flags equals to hashtable buffer size (plus one for zero value).
    /// Changes in hash table broke correspondence,
    /// mutable JoinStuff::JoinUsedFlags used_flags;

    SERDE Type hash_method_type;

    /// Only SERDE when emit_changelog is true
    SERDE std::optional<JoinResults> join_results;

    bool retract_push_down = false;
    bool emit_changelog = false;
    bool bidirectional_hash_join = true;
    bool range_bidirectional_hash_join = true;

    /// Delta column in right-left-join
    /// `rlj` -> right-left-join
    std::optional<size_t> left_delta_column_position_rlj;
    std::optional<size_t> right_delta_column_position_rlj;

    UInt64 join_max_cached_bytes = 0;

    Block output_header;

    /// Combined timestamp watermark progression of left stream and right stream
    SERDE std::atomic_int64_t combined_watermark = 0;

    struct JoinGlobalMetrics
    {
        size_t total_join = 0;
        size_t left_block_and_right_range_bucket_no_intersection_skip = 0;
        size_t right_block_and_left_range_bucket_no_intersection_skip = 0;

        std::string string() const
        {
            return fmt::format(
                "total_join={} "
                "left_block_and_right_range_bucket_no_intersection_skip={} right_block_and_left_range_bucket_no_intersection_skip={}",
                total_join,
                left_block_and_right_range_bucket_no_intersection_skip,
                right_block_and_left_range_bucket_no_intersection_skip);
        }
    };
    friend void serialize(const JoinGlobalMetrics & join_metrics, WriteBuffer & wb);
    friend void deserialize(JoinGlobalMetrics & join_metrics, ReadBuffer & rb);

    SERDE JoinGlobalMetrics join_metrics;

    Poco::Logger * logger;
};

struct HashJoinMapsVariants
{
    size_t size(const HashJoin * join) const;
    std::vector<HashJoin::MapsVariant> map_variants;
};

}
}
