#pragma once

#include <Interpreters/Streaming/HashJoin/HashJoin.h>
#include <Interpreters/Streaming/HashJoin/JoinStreamDescription.h>
#include <Interpreters/Streaming/HashJoin/MemoryHashJoin/RowRefs.h>
#include <Interpreters/Streaming/HashJoin/MemoryHashJoin/joinData.h>
#include <Interpreters/Streaming/HashJoin/joinKind.h>

#include <Interpreters/TableJoin.h>
#include <Common/ColumnUtils.h>
#include <Common/HashMapSizes.h>
#include <Common/HashMapsTemplate.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ProtonCommon.h>
#include <Common/serde.h>

namespace DB::Streaming
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

class MemoryHashJoin final : public HashJoin
{
public:
    MemoryHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        JoinStreamDescriptionPtr left_join_stream_desc_,
        JoinStreamDescriptionPtr right_join_stream_desc_);

    ~MemoryHashJoin() noexcept override;

    HashJoinType type() const noexcept override { return HashJoinType::Memory; }

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

    bool isFilled() const override { return false; }

    /// For RIGHT and FULL JOINs.
    /// A stream that will contain default values from left table, joined with rows from right table, that was not joined before.
    /// Use only after all calls to joinBlock was done.
    /// left_sample_block is passed without account of 'use_nulls' setting (columns will be converted to Nullable inside).
    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    String metricsString() const override;

    /// Number of keys in all built JOIN maps.
    size_t getTotalRowCount() const override;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
    size_t getTotalByteCount() const override;

    bool alwaysReturnsEmptySet() const override;

    size_t dataBlockSize() const noexcept { return table_join->dataBlockSize(); }

    void serialize(WriteBuffer & wb, VersionType version) const override;
    void deserialize(ReadBuffer & rb, VersionType version) override;

    void cancel() override { }

    using RefListMultiple = RowRefListMultiple<JoinDataBlock>;
    using RefListMultipleRef = RowRefListMultipleRef<JoinDataBlock>;
    using RefListMultipleRefPtr = RowRefListMultipleRefPtr<JoinDataBlock>;

    using MapsOne = HashMapsTemplate<RowRefWithRefCount<JoinDataBlock>, /*skip_dtor_on_destroy=*/true>;
    using MapsMultiple = HashMapsTemplate<RowRefListMultiplePtr<JoinDataBlock>>;
    using MapsAll = HashMapsTemplate<RowRefList<JoinDataBlock>>;
    using MapsAsof = HashMapsTemplate<AsofRowRefs<JoinDataBlock>>;
    using MapsRangeAsof = HashMapsTemplate<RangeAsofRowRefs<JoinDataBlock>>;
    using MapsVariant = std::variant<MapsOne, MapsAll, MapsAsof, MapsRangeAsof, MapsMultiple>;

    HashMapSizes sizesOfMapsVariant(const MapsVariant & maps_variant) const;

    /// bool isUsed(size_t off) const { return used_flags.getUsedSafe(off); }
    /// bool isUsed(const Block * block_ptr, size_t row_idx) const { return used_flags.getUsedSafe(block_ptr, row_idx); }

    friend struct BufferedStreamData;

    /// For changelog emit
    struct JoinResults
    {
        JoinResults(const Block & header_);

        String joinMetricsString(const MemoryHashJoin * join) const;

        mutable std::mutex mutex;

        Block sample_block;

        SERDE CachedBlockMetrics metrics;
        SERDE JoinDataBlockList blocks;

        /// Arena pool to hold the (string) keys
        Arena pool;

        /// Building hash map for joined blocks, then we can find previous
        /// join blocks quickly by using joined keys
        SERDE std::unique_ptr<HashJoinMapsVariants> maps;

        void serialize(WriteBuffer & wb, VersionType version, const MemoryHashJoin & join) const;
        void deserialize(ReadBuffer & rb, VersionType version, const MemoryHashJoin & join);
    };

private:
    void initBufferedData();
    void initHashMaps(std::vector<MapsVariant> & all_maps);

    /// For versioned kv / changelog kv
    void initLeftPrimaryKeyHashTable();
    void initRightPrimaryKeyHashTable();

    void validateAsofJoinKey();

    void chooseHashMethod();

    void checkLimits() const;

    /// For range bidirectional hash join
    template <bool is_left_block>
    std::vector<Block> insertBlockToRangeBucketsAndJoin(Block block);

    template <bool is_left_block>
    void doInsertBlock(Block block, HashBlocksPtr target_hash_blocks, IColumn::Filter * new_keys_filter = nullptr);

    /// For bidirectional hash join
    /// Return retracted block if needs emit changelog, otherwise empty block
    template <bool is_left_block>
    Block joinBlockWithHashTable(Block & block, HashBlocksPtr target_hash_blocks);

    /// Provide an API to join with join_kind instead of using member variable streaming_kind
    template <bool is_left_block>
    Block joinBlockWithHashTable(Block & block, HashBlocksPtr target_hash_blocks, Kind join_kind);

    template <bool is_left_block>
    void doJoinBlockWithHashTable(Block & block, HashBlocksPtr target_hash_blocks);

    template <bool is_left_block>
    void doJoinBlockWithHashTable(Block & block, HashBlocksPtr target_hash_blocks, Kind join_kind);

    template <bool is_left_block>
    void doJoinBlockWithHashTables(Block & block, HashBlocksPtrs target_hash_blocks, Kind join_kind);

    /// Join left block with right hash table(s) or join right block with left hash table(s)
    template <bool is_left_block, Kind KIND, Strictness STRICTNESS, typename Map>
    void joinBlockImpl(Block & block, const Block & block_with_columns_to_add, const std::vector<std::vector<const Map *>> & map_vv) const;

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
    std::vector<RefListMultipleRef *> eraseOrAppendForPartialPrimaryKeyJoin(const Block & block);

    template <typename KeyGetter, typename Map>
    std::vector<RefListMultipleRef *> eraseOrAppendForPartialPrimaryKeyJoin(Map & map, ColumnRawPtrs && primary_key_columns);

    /// If the left / right_block is a retraction block : rows in `_tp_delta` column all have `-1`
    /// We erase the previous key / values from hash table
    /// Use for append JOIN changelog_kv / versioned_kv case
    /// @return true if keys are erased, otherwise false
    struct JoinData;
    template <bool is_left_block>
    void eraseExistingKeys(Block & block, JoinData & join_data);

    template <typename KeyGetter, typename Map>
    void doEraseExistingKeys(
        Map & map,
        const DB::Block & right_block,
        ColumnRawPtrs && key_columns,
        const std::vector<size_t> & key_size,
        const std::vector<size_t> & skip_columns,
        ConstNullMapPtr null_map,
        bool delete_key);

    /// For bidirectional join, the input is a retract block
    template <bool is_left_block>
    std::optional<Block> eraseExistingKeysAndRetractJoin(Block & left_block);

    template <bool is_left_block>
    void transformToOutputBlock(Block & joined_block) const;

private:
    /// versioned-kv and changelog-kv both needs define a primary key
    /// If join on partial primary key columns (or no primary key column is expressed in join on clause),
    /// init this data structure
    struct PrimaryKeyHashTable
    {
        PrimaryKeyHashTable(HashType hash_method_type_, Sizes && key_size_);

        HashType hash_method_type;

        Sizes key_size;

        /// For key allocation
        Arena pool;

        /// Hash maps variants indexed by primary key columns
        SERDE HashMapsTemplate<RefListMultipleRefPtr> map;
    };
    using PrimaryKeyHashTablePtr = std::shared_ptr<PrimaryKeyHashTable>;

    struct JoinData
    {
        explicit JoinData(JoinContext & join_ctx_) : join_ctx(join_ctx_) { }

        void serialize(WriteBuffer & wb, VersionType version) const;
        void deserialize(ReadBuffer & rb, VersionType version);

        JoinContext & join_ctx;

        /// Only for kv join. Used to maintain all unique keys
        PrimaryKeyHashTablePtr primary_key_hash_table;

        SERDE BufferedStreamDataPtr buffered_data;
    };

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

    SERDE HashType hash_method_type;

    /// Only SERDE when emit_changelog is true
    SERDE std::optional<JoinResults> join_results;
};

struct HashJoinMapsVariants
{
    /// \return Number of keys in the map
    HashMapSizes sizes(const MemoryHashJoin * join) const;
    std::vector<MemoryHashJoin::MapsVariant> map_variants;
};

/// Serialize/Deserialize both `JoinDataBlockList` and `HashJoinMapsVariants`
void serializeHashJoinMapsVariants(
    const JoinDataBlockList & blocks,
    const HashJoinMapsVariants & maps,
    WriteBuffer & wb,
    VersionType version,
    const Block & header,
    const MemoryHashJoin & join,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices = nullptr);

void deserializeHashJoinMapsVariants(
    JoinDataBlockList & blocks,
    HashJoinMapsVariants & maps,
    ReadBuffer & rb,
    VersionType version,
    Arena & pool,
    const Block & header,
    const MemoryHashJoin & join,
    DeserializedIndicesToRowRefListMultiple<JoinDataBlock> * deserialized_indices_to_multiple_ref = nullptr);

}
