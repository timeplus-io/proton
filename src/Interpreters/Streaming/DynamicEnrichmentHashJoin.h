#pragma once

#include <Interpreters/Streaming/HashJoin.h>

namespace DB
{
namespace Streaming
{
class DynamicEnrichmentHashJoin final : public HashJoin
{
public:
    using HashJoin:HashJoin;

    ~DynamicEnrichmentHashJoin() noexcept override { }

    /// \returns <retracted_block, joined_block>
    std::pair<LightChunk, LightChunk> insertLeftAndJoin(LightChunk && chunk) override;
    std::pair<LightChunk, LightChunk> insertRightDataBlockAndJoin(LightChunk && chunk) override;

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

    size_t dataBlockSize() const noexcept { return table_join->dataBlockSize(); }
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

    using RefListMultiple = RowRefListMultiple<JoinDataBlock>;
    using RefListMultipleRef = RowRefListMultipleRef<JoinDataBlock>;
    using RefListMultipleRefPtr = RowRefListMultipleRefPtr<JoinDataBlock>;

    using MapsOne = HashMapsTemplate<RowRefWithRefCount<JoinDataBlock>>;
    using MapsMultiple = HashMapsTemplate<RowRefListMultiplePtr<JoinDataBlock>>;
    using MapsAll = HashMapsTemplate<RowRefList<JoinDataBlock>>;
    using MapsAsof = HashMapsTemplate<AsofRowRefs<JoinDataBlock>>;
    using MapsRangeAsof = HashMapsTemplate<RangeAsofRowRefs<JoinDataBlock>>;
    using MapsVariant = std::variant<MapsOne, MapsAll, MapsAsof, MapsRangeAsof, MapsMultiple>;

    HashMapSizes sizesOfMapsVariant(const MapsVariant & maps_variant) const;
    HashType getHashMethodType() const { return hash_method_type; }

    /// bool isUsed(size_t off) const { return used_flags.getUsedSafe(off); }
    /// bool isUsed(const Block * block_ptr, size_t row_idx) const { return used_flags.getUsedSafe(block_ptr, row_idx); }

    friend struct BufferedStreamData;

    /// For changelog emit
    struct JoinResults
    {
        JoinResults(const Block & header_) : sample_block(header_), blocks(metrics), maps(std::make_unique<HashJoinMapsVariants>()) { }

        String joinMetricsString(const HashJoin * join) const;

        mutable std::mutex mutex;

        Block sample_block;

        SERDE CachedBlockMetrics metrics;
        SERDE JoinDataBlockList blocks;

        /// Arena pool to hold the (string) keys
        Arena pool;

        /// Building hash map for joined blocks, then we can find previous
        /// join blocks quickly by using joined keys
        SERDE std::unique_ptr<HashJoinMapsVariants> maps;
    };

    JoinStreamDescriptionPtr leftJoinStreamDescription() const noexcept override { return left_data.join_stream_desc; }
    JoinStreamDescriptionPtr rightJoinStreamDescription() const noexcept override { return right_data.join_stream_desc; }

private:
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
    void doInsertBlock(Block block, HashBlocksPtr target_hash_blocks, std::vector<RefListMultipleRef *> row_refs = {});

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
    std::vector<RefListMultipleRef *> eraseOrAppendForPartialPrimaryKeyJoin(const Block & block);

    template <typename KeyGetter, typename Map>
    std::vector<RefListMultipleRef *> eraseOrAppendForPartialPrimaryKeyJoin(Map & map, ColumnRawPtrs && primary_key_columns);

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

    template <bool is_left_block>
    void transformToOutputBlock(Block & joined_block) const;

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
        explicit JoinData(JoinStreamDescriptionPtr join_stream_desc_) : join_stream_desc(std::move(join_stream_desc_))
        {
            assert(join_stream_desc);
        }

        JoinStreamDescriptionPtr join_stream_desc;

        ColumnWithTypeAndName asof_key_column;

        /// Only for kv join. Used to maintain all unique keys
        PrimaryKeyHashTablePtr primary_key_hash_table;

        SERDE BufferedStreamDataPtr buffered_data;

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

    SERDE HashType hash_method_type;

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

    /// `lrj` -> left-right-join
    std::optional<size_t> left_delta_column_position_lrj;
    std::optional<size_t> right_delta_column_position_lrj;

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
    /// \return Number of keys in the map
    HashMapSizes sizes(const HashJoin * join) const;
    std::vector<HashJoin::MapsVariant> map_variants;
};

}
}
