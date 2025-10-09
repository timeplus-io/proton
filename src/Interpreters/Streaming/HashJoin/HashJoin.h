#pragma once

#include <Interpreters/Streaming/HashJoin/IHashJoin.h>
#include <Interpreters/Streaming/HashJoin/joinKind.h>
#include <Interpreters/TableJoin.h>
#include <Common/serde.h>

#include <boost/container_hash/hash.hpp>

namespace DB::Streaming
{

class HashJoin : public IHashJoin
{
public:
    /// <left_data_storage_semantic, join_kind, join_strictness, right_data_storage_semantic> - bool
    using JoinCombinationType = std::tuple<StorageSemantic, JoinKind, JoinStrictness, StorageSemantic>;
    using SupportMatrix = std::unordered_map<JoinCombinationType, bool, boost::hash<JoinCombinationType>>;
    static const SupportMatrix support_matrix;

    HashJoin(
        std::shared_ptr<TableJoin> table_join_,
        JoinStreamDescriptionPtr left_join_stream_desc_,
        JoinStreamDescriptionPtr right_join_stream_desc_,
        const std::string & logger_name);

    /// So far we only support these join combinations
    static void validate(const JoinCombinationType & join_combination);

    JoinStreamDescriptionPtr leftJoinStreamDescription() const noexcept override { return left_join_ctx.join_stream_desc; }
    JoinStreamDescriptionPtr rightJoinStreamDescription() const noexcept override { return right_join_ctx.join_stream_desc; }

    bool emitChangeLog() const override { return emit_changelog; }
    bool bidirectionalHashJoin() const override { return bidirectional_hash_join; }
    bool rangeBidirectionalHashJoin() const override { return range_bidirectional_hash_join; }
    bool leftStreamRequiresBufferingDataToAlign() const override { return range_bidirectional_hash_join; }
    bool rightStreamRequiresBufferingDataToAlign() const override
    {
        return streaming_strictness == Strictness::Asof || range_bidirectional_hash_join;
    }

    const TableJoin & getTableJoin() const override { return *table_join; }

    const ColumnWithTypeAndName & rightAsofKeyColumn() const { return right_join_ctx.asof_key_column; }
    const ColumnWithTypeAndName & leftAsofKeyColumn() const { return left_join_ctx.asof_key_column; }

    UInt64 keepVersions() const { return right_join_ctx.join_stream_desc->keep_versions; }

    const Block & getOutputHeader() const { return output_header; }

    JoinKind getKind() const { return kind; }
    JoinStrictness getStrictness() const { return strictness; }
    Kind getStreamingKind() const { return streaming_kind; }
    Strictness getStreamingStrictness() const { return streaming_strictness; }
    const std::optional<TypeIndex> & getAsofType() const { return asof_type; }
    ASOFJoinInequality getAsofInequality() const { return asof_inequality; }
    bool anyTakeLastRow() const { return any_take_last_row; }

    void checkTypesOfKeys(const Block & block) const override;

    void getKeyColumnPositions(
        std::vector<size_t> & left_key_column_positions,
        std::vector<size_t> & right_key_column_positions,
        bool include_asof_key_column) const override;

protected:
    void initLeftBlockStructure();
    void initRightBlockStructure();

    struct JoinContext;
    void initBlockStructure(JoinContext & join_data) const;

    const Block & savedLeftBlockSample() const { return left_join_ctx.sample_block; }
    const Block & savedRightBlockSample() const { return right_join_ctx.sample_block; }

    template <bool is_left_block>
    Block prepareBlock(const Block & block) const
    {
        if constexpr (is_left_block)
            return prepareBlockToSave(block, savedLeftBlockSample());
        else
            return prepareBlockToSave(block, savedRightBlockSample());
    }

    /// Remove columns which are not needed to be projected
    static Block prepareBlockToSave(const Block & block, const Block & sample_block);

    struct AsofJoinKeyColumnInfo
    {
        UInt32 scale = 0;
        TypeIndex type_index;

        size_t left_col_pos = 0;
        size_t right_col_pos = 0;
    };

    std::optional<AsofJoinKeyColumnInfo> doValidateAsofJoinKey() const;

    bool isRetractBlock(const Block & block, const JoinStreamDescription & join_stream_desc);

    template <bool is_left_block>
    void addMissingColumns(Block & block) const;

private:
    void init();
    void checkJoinSemantic() const;
    void reviseJoinStrictness();

protected:
    struct JoinContext
    {
        explicit JoinContext(JoinStreamDescriptionPtr join_stream_desc_) : join_stream_desc(std::move(join_stream_desc_))
        {
            assert(join_stream_desc);
        }

        JoinStreamDescriptionPtr join_stream_desc;

        ColumnWithTypeAndName asof_key_column;

        Block sample_block; /// Block as it would appear in the BlockList or in hash table mapped values

        /// Block with columns from the (left or) right-side table except key columns.
        Block sample_block_with_columns_to_add;
        /// Block with key columns in the same order they appear in the (left or) right-side table (duplicates appear once).
        Block table_keys;
        /// Block with key columns (left or) right-side table keys that are needed in result (would be attached after joined columns).
        Block required_keys;
        /// Left table column names that are sources for required_right_keys columns
        std::vector<String> required_keys_sources;

        std::optional<std::vector<size_t>> reserved_column_positions; /// `_tp_delta` etc column positions in sample block if they exist

        bool validated_join_key_types = false;
    };

protected:
    /// Only SERDE the clauses of join
    SERDE std::shared_ptr<TableJoin> table_join;
    JoinKind kind;
    JoinStrictness strictness;
    JoinContext left_join_ctx;
    JoinContext right_join_ctx;

    SERDE Kind streaming_kind;
    SERDE Strictness streaming_strictness;

    bool any_take_last_row; /// Overwrite existing values when encountering the same key again
    /// Only SERDE for ASOF JOIN or RANGE JOIN
    SERDE std::optional<TypeIndex> asof_type;
    SERDE ASOFJoinInequality asof_inequality;

    /// Cache data members which avoid re-computation for every join
    std::vector<std::pair<String, String>> cond_column_names;

    SERDE std::vector<std::vector<size_t>> key_sizes;

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

        void serialize(WriteBuffer & wb, VersionType version) const;
        void deserialize(ReadBuffer & rb, VersionType version);
    };

    SERDE JoinGlobalMetrics join_metrics;

    LoggerPtr logger;
};

}
