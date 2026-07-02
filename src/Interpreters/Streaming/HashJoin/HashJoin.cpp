#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/HashJoin/AddedColumns.h>
#include <Interpreters/Streaming/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int UNSUPPORTED;
}

namespace Streaming
{
/// [INNER] JOIN : only matching rows are returned
/// LEFT [OUTER] JOIN: non-matching rows from left stream are returned in addition to matching rows
/// RIGHT [OUTER] JOIN: non-matching rows from right stream are returned in addition to matching rows
/// FULL [OUTER] JOIN: non-matching rows from both streams are returned in addition to matching rows
/// CROSS JOIN: produces a cartesian product of the 2 streams. "join keys" are not specified
///
/// Non-standard join types:
/// INNER LATEST JOIN : convert left / right stream to (versioned) kv and execute versioned kv join after
/// LEFT / RIGHT ANY JOIN: partially (for opposite side of LEFT and RIGHT) disables the cartesian product for standard JOIN types
/// INNER ANY JOIN: completely disables the cartesian product for standard JOIN types
/// ASOF JOIN: Closest match (non-equal) join
/// LEFT ASOF JOIN : non-matching rows from left stream are returned in addition to ASOF JOIN matching rows
/// LEFT / RIGHT SEMI JOIN: a whitelist on "join keys", without producing a cartesian product
/// LEFT / RIGHT ANTI JOIN: a blacklist on "join keys", without producing a cartesian product

/// Difference between `LEFT SEMI` and `LEFT ANY`
/// `LEFT SEMI` only produce matching rows on join keys and one left row only joins one right row
/// `LEFT ANY` produce non-matching rows from left stream + `left semi`

/// `ANTI JOIN` looks weird in streaming join but in some troubleshooting scenarios in historical join, it is handy since it is returning non-matching rows only
/// `left anti`: if there are non-matching rows from left stream, returning these non-matching rows with default value filling for right projected columns
/// `right anti`: if there are non-matching rows from right stream, returning these non-matching rows with default value filling left projected columns

/// A 4 dimensions array : Left StorageSemantic, Kind, Strictness, Right StorageSemantic
/// There are 4 * 6 * 5 * 4 = 480 total combinations
const HashJoin::SupportMatrix HashJoin::support_matrix = {
    /// <left_storage_semantic, join_kind, join_strictness, right_storage_semantic> - supported
    /// Append ...
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::All, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::All, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Asof, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Asof, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Asof, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Any, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Any, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Any, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::All, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Asof, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Asof, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Asof, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::Append, JoinKind::Full, JoinStrictness::Any, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Full, JoinStrictness::Any, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Full, JoinStrictness::Any, StorageSemantic::NativeKV}, true},

    /// Anti JOIN
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Anti, StorageSemantic::Append}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Anti, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Append, JoinKind::Left, JoinStrictness::Anti, StorageSemantic::NativeKV}, true},

    /// Changelog ...
    /* <=> Changelog inner all join Changelog */
    {{StorageSemantic::Changelog, JoinKind::Inner, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Inner, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::Changelog, JoinKind::Inner, JoinStrictness::All, StorageSemantic::NativeKV}, true},
    /* <=> Changelog left all join Changelog */
    {{StorageSemantic::Changelog, JoinKind::Left, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Left, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Left, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::Changelog, JoinKind::Left, JoinStrictness::All, StorageSemantic::NativeKV}, true},
    /* <=> Changelog full all join Changelog */
    {{StorageSemantic::Changelog, JoinKind::Full, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Full, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::Changelog, JoinKind::Full, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::Changelog, JoinKind::Full, JoinStrictness::All, StorageSemantic::NativeKV}, true},

    /// ChangelogKV ...
    /* <=> Changelog inner all join Changelog */
    {{StorageSemantic::ChangelogKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::NativeKV}, true},
    /* <=> Changelog left all join Changelog */
    {{StorageSemantic::ChangelogKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::NativeKV}, true},
    /* <=> Changelog full all join Changelog */
    {{StorageSemantic::ChangelogKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::ChangelogKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::NativeKV}, true},

    /// VersionedKV ...
    /* <=> Changelog inner all join Changelog */
    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::NativeKV}, true},
    /* <=> Changelog left all join Changelog */
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::NativeKV}, true},
    /* <=> Changelog full all join Changelog */
    {{StorageSemantic::VersionedKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::Append}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::VersionedKV, JoinKind::Full, JoinStrictness::Any, StorageSemantic::Append}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Full, JoinStrictness::Any, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Full, JoinStrictness::Any, StorageSemantic::NativeKV}, true},

    /// (VersionedKV as Append) Anti JOIN
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::Anti, StorageSemantic::Append}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::Anti, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::VersionedKV, JoinKind::Left, JoinStrictness::Anti, StorageSemantic::NativeKV}, true},

    /// NativeKV ...
    /* <=> Changelog inner all join Changelog */
    {{StorageSemantic::NativeKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::NativeKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::NativeKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::NativeKV, JoinKind::Inner, JoinStrictness::All, StorageSemantic::NativeKV}, true},
    /* <=> Changelog left all join Changelog */
    {{StorageSemantic::NativeKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::NativeKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::NativeKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::NativeKV, JoinKind::Left, JoinStrictness::All, StorageSemantic::NativeKV}, true},
    /* <=> Changelog full all join Changelog */
    {{StorageSemantic::NativeKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::ChangelogKV}, true},
    {{StorageSemantic::NativeKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::NativeKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::Changelog}, true},
    {{StorageSemantic::NativeKV, JoinKind::Full, JoinStrictness::All, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::NativeKV, JoinKind::Full, JoinStrictness::Any, StorageSemantic::Append}, true},
    {{StorageSemantic::NativeKV, JoinKind::Full, JoinStrictness::Any, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::NativeKV, JoinKind::Full, JoinStrictness::Any, StorageSemantic::NativeKV}, true},

    {{StorageSemantic::NativeKV, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::Append}, true},
    {{StorageSemantic::NativeKV, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::NativeKV, JoinKind::Inner, JoinStrictness::Any, StorageSemantic::NativeKV}, true},

    /// (NativeKV as Append) Anti JOIN
    {{StorageSemantic::NativeKV, JoinKind::Left, JoinStrictness::Anti, StorageSemantic::Append}, true},
    {{StorageSemantic::NativeKV, JoinKind::Left, JoinStrictness::Anti, StorageSemantic::VersionedKV}, true},
    {{StorageSemantic::NativeKV, JoinKind::Left, JoinStrictness::Anti, StorageSemantic::NativeKV}, true},
};

void HashJoin::validate(const JoinCombinationType & join_combination)
{
    auto iter = support_matrix.find(join_combination);
    if (iter == support_matrix.end() || !(iter->second))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "'{}' {} {} JOIN '{}' is not supported",
            magic_enum::enum_name(std::get<0>(join_combination)),
            toString(std::get<1>(join_combination)),
            toString(std::get<2>(join_combination)),
            magic_enum::enum_name(std::get<3>(join_combination)));
}

HashJoin::HashJoin(
    std::shared_ptr<TableJoin> table_join_,
    JoinStreamDescriptionPtr left_join_stream_desc_,
    JoinStreamDescriptionPtr right_join_stream_desc_,
    const std::string & logger_name)
    : table_join(std::move(table_join_))
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , left_join_ctx(std::move(left_join_stream_desc_))
    , right_join_ctx(std::move(right_join_stream_desc_))
    , any_take_last_row(false)
    , asof_inequality(table_join->getAsofInequality())
    , logger(getLogger(logger_name))
{
    init();

    initRightBlockStructure();
}

void HashJoin::init()
{
    checkJoinSemantic();

    streaming_kind = Streaming::toJoinKind(kind);
    streaming_strictness = Streaming::toJoinStrictness(strictness, table_join->isRangeJoin());

    emit_changelog = table_join->emitChangeLog();

    retract_push_down
        = (isKeyedStorage(left_join_ctx.join_stream_desc->data_stream_semantic)
           && isKeyedStorage(right_join_ctx.join_stream_desc->data_stream_semantic) && right_join_ctx.join_stream_desc->hasPrimaryKey());

    /// So far there are following cases which doesn't require bidirectional join
    /// SELECT * FROM append_only INNER ALL JOIN versioned_kv ON append_only.key = versioned_kv.key;
    /// SELECT * FROM append_only INNER LATEST JOIN versioned_kv ON append_only.key = versioned_kv.key;
    /// SELECT * FROM append_only INNER ASOF JOIN versioned_kv ON append_only.key = versioned_kv.key AND append_only.timestamp < versioned_kv.timestamp SETTINGS keep_versions=3;
    /// SELECT * FROM left_append_only INNER ASOF JOIN right_append_only ON left_append_only.key = right_append_only.key AND left_append_only.timestamp < right_append_only.timestamp SETTINGS keep_versions=3;
    /// SELECT * FROM left_append_only INNER LATEST JOIN right_append_only ON left_append_only.key = right_append_only.key;
    /// `ASOF` keeps multiple versions and `LATEST` only keeps the latest version for the join key
    /// Also for `Stream join Table`
    auto data_enrichment_join = (left_join_ctx.join_stream_desc->data_stream_semantic == DataStreamSemantic::Append
                                 && right_join_ctx.join_stream_desc->data_stream_semantic == DataStreamSemantic::Changelog)
        || streaming_strictness == Strictness::Asof || (streaming_strictness == Strictness::Latest && streaming_kind != Kind::Full)
        || streaming_strictness == Strictness::Anti || !right_join_ctx.join_stream_desc->data_stream_semantic.streaming;

    bidirectional_hash_join = !data_enrichment_join;

    /// append-only inner/left join append-only on ... and date_diff_within(10s)
    /// In case when emitChangeLog()
    if (streaming_strictness == Strictness::Range
        && (left_join_ctx.join_stream_desc->data_stream_semantic != DataStreamSemantic::Append
            || right_join_ctx.join_stream_desc->data_stream_semantic != DataStreamSemantic::Append
            || (streaming_kind != Kind::Inner && streaming_kind != Kind::Left)))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Only inner/left range join is supported and the left and right stream must be append-only streams in range join");

    range_bidirectional_hash_join = bidirectional_hash_join && (streaming_strictness == Strictness::Range);

    reviseJoinStrictness();

    /// Cache condition column names
    const auto & on_exprs = table_join->getClauses();
    cond_column_names.reserve(on_exprs.size());
    for (const auto & on_expr : on_exprs)
        cond_column_names.push_back(on_expr.condColumnNames());

    if (table_join->oneDisjunct())
    {
        const auto & key_names_right = table_join->getOnlyClause().key_names_right;
        JoinCommon::splitAdditionalColumns(
            key_names_right,
            right_join_ctx.join_stream_desc->input_header,
            right_join_ctx.table_keys,
            right_join_ctx.sample_block_with_columns_to_add);
        right_join_ctx.required_keys = table_join->getRequiredRightKeys(right_join_ctx.table_keys, right_join_ctx.required_keys_sources);
    }
    else
    {
        /// required right keys concept does not work well if multiple disjuncts, we need all keys
        /// SELECT * FROM left JOIN right ON left.key = right.key OR left.value = right.value
        right_join_ctx.sample_block_with_columns_to_add = right_join_ctx.table_keys
            = materializeBlock(right_join_ctx.join_stream_desc->input_header);
    }
}

void HashJoin::initLeftBlockStructure()
{
    assert(bidirectionalHashJoin());

    JoinCommon::convertToFullColumnsInplace(left_join_ctx.table_keys);

    initBlockStructure(left_join_ctx);

    /// Remove un-needed left `key` columns
    /// SELECT left.key FROM left INNER / LEFT / RIGHT JOIN right ON left.key = right.key
    /// right_data.required_keys : []
    /// left_data.required_keys : [left.key]
    /// When we use right block to join left blocks, right block already has `right.key`,
    /// we don't need add `left.key` to the joined block since we can simply "rename" `right.key` to `left.key`.
    /// Actually there are 3 cases
    /// 1) INNER JOIN, renaming is just ok since the keys are equal when they are joined
    /// 2) LEFT JOIN, renaming is just ok as well. Joined means equal, not joined empty result
    /// 3) RIGHT JOIN, renaming will not be OK for non-joined rows (FIXME)
    JoinCommon::createMissedColumns(left_join_ctx.sample_block_with_columns_to_add);

    /// right_data.required_keys + right_data.sample_block_with_column_to_add is the projection header
    /// left_data.required_keys + left_data.sample_block_with_column_to_add is the projection header

    if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof)
        /// It should be nullable if nullable_right_side is true
        left_join_ctx.asof_key_column = savedLeftBlockSample().getByName(table_join->getOnlyClause().key_names_left.back());
}

void HashJoin::initRightBlockStructure()
{
    JoinCommon::convertToFullColumnsInplace(right_join_ctx.table_keys);

    initBlockStructure(right_join_ctx);

    JoinCommon::createMissedColumns(right_join_ctx.sample_block_with_columns_to_add);

    if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof)
        /// It should be nullable if nullable_right_side is true
        right_join_ctx.asof_key_column = savedRightBlockSample().getByName(table_join->getOnlyClause().key_names_right.back());
}

void HashJoin::initBlockStructure(JoinContext & join_ctx) const
{
    Block & saved_block_sample = join_ctx.sample_block;

    bool multiple_disjuncts = !table_join->oneDisjunct();
    /// We could remove key columns for LEFT | INNER HashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns = !table_join->isEnabledAlgorithm(JoinAlgorithm::AUTO) || isRightOrFull(kind) || multiple_disjuncts;
    if (save_key_columns)
        saved_block_sample = join_ctx.table_keys.cloneEmpty();
    else if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof)
        /// Save ASOF key
        saved_block_sample.insert(join_ctx.table_keys.safeGetByPosition(join_ctx.table_keys.columns() - 1));

    /// Save non key columns
    for (const auto & column : join_ctx.sample_block_with_columns_to_add)
    {
        if (auto * col = saved_block_sample.findByName(column.name))
            *col = column;
        else
            saved_block_sample.insert(column);
    }

    /// Cache delta position in saved block for future use
    std::vector<size_t> reserved_column_positions;
    if (join_ctx.join_stream_desc->hasDeltaColumn())
    {
        const auto & delta_column_name = join_ctx.join_stream_desc->deltaColumnName();

        if (saved_block_sample.has(delta_column_name))
            reserved_column_positions.push_back(saved_block_sample.getPositionByName(delta_column_name));
    }

    for (size_t pos = 0; const auto & col : saved_block_sample)
    {
        if (col.name == ProtonConsts::RESERVED_EVENT_TIME || col.name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID
            || col.name == ProtonConsts::RESERVED_APPEND_TIME || col.name == ProtonConsts::RESERVED_INDEX_TIME)
            reserved_column_positions.push_back(pos);
        else if (
            col.name.ends_with(ProtonConsts::RESERVED_EVENT_TIME) || col.name.ends_with(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID)
            || col.name.ends_with(ProtonConsts::RESERVED_APPEND_TIME) || col.name.ends_with(ProtonConsts::RESERVED_INDEX_TIME))
            reserved_column_positions.push_back(pos);

        ++pos;
    }

    if (!reserved_column_positions.empty())
        join_ctx.reserved_column_positions.emplace(std::move(reserved_column_positions));
}

Block HashJoin::prepareBlockToSave(const Block & block, const Block & sample_block)
{
    Block structured_block;

    structured_block.info = block.info;

    for (const auto & sample_column : sample_block.getColumnsWithTypeAndName())
    {
        ColumnWithTypeAndName column = block.getByName(sample_column.name);
        if (sample_column.column->isNullable())
            JoinCommon::convertColumnToNullable(column);

        if (column.column->lowCardinality() && !sample_column.column->lowCardinality())
        {
            column.column = column.column->convertToFullColumnIfLowCardinality();
            column.type = removeLowCardinality(column.type);
        }

        /// There's no optimization for right side const columns. Remove const-ness if any.
        column.column = recursiveRemoveSparse(column.column->convertToFullColumnIfConst());
        structured_block.insert(std::move(column));
    }

    return structured_block;
}

void HashJoin::getKeyColumnPositions(
    std::vector<size_t> & left_key_column_positions, std::vector<size_t> & right_key_column_positions, bool include_asof_key_column) const
{
    auto calc_key_positions = [](const auto & key_column_names, const auto & header, auto & key_column_positions) {
        key_column_positions.reserve(key_column_names.size());
        for (const auto & name : key_column_names)
            key_column_positions.push_back(header.getPositionByName(name));
    };

    calc_key_positions(table_join->getOnlyClause().key_names_left, left_join_ctx.join_stream_desc->input_header, left_key_column_positions);
    calc_key_positions(
        table_join->getOnlyClause().key_names_right, right_join_ctx.join_stream_desc->input_header, right_key_column_positions);

    if (!include_asof_key_column && (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof))
    {
        left_key_column_positions.pop_back();
        right_key_column_positions.pop_back();
    }

    assert(!left_key_column_positions.empty());
    assert(!right_key_column_positions.empty());
}

void HashJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_join_ctx.table_keys, onexpr.key_names_right);
}

void HashJoin::checkJoinSemantic() const
{
    if (!table_join->oneDisjunct())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Stream to Stream join only supports only one disjunct join clause");

    /// For streaming join, we don't support inline JOIN ON predicate like `left.value > 10` in the following query example
    /// since stream query will need buffer more additional non-joined data in-memory
    /// SELECT * FROM left INNER JOIN right ON left.key = right.key AND left.value > 10 and right.value > 80;
    /// User shall use WHERE predicate like
    /// SELECT * FROM (SELECT * FROM left WHERE left.value > 10) as left INNER JOIN right ON left.key = right.key
    for (const auto & on_expr : table_join->getClauses())
    {
        const auto & on_expr_cond_column_names = on_expr.condColumnNames();
        if (!on_expr_cond_column_names.first.empty() || !on_expr_cond_column_names.second.empty())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Streaming join doesn't support predicates in JOIN ON clause. Use WHERE predicate instead");
    }

    validate(
        {left_join_ctx.join_stream_desc->data_stream_semantic.toStorageSemantic(),
         kind,
         strictness,
         right_join_ctx.join_stream_desc->data_stream_semantic.toStorageSemantic()});
}

void HashJoin::reviseJoinStrictness()
{
    /// Strictness::Any in streaming join means take the latest row to join.
    any_take_last_row = (streaming_strictness == Strictness::Latest || streaming_strictness == Strictness::Anti);

    /// For explicit asof / latest / anti join, we honor what end users like to have
    if (streaming_strictness == Strictness::Asof || streaming_strictness == Strictness::Latest || streaming_strictness == Strictness::Anti)
        return;

    if (streaming_strictness == Strictness::Range)
    {
        if (!isAppendDataStream(right_join_ctx.join_stream_desc->data_stream_semantic))
            throw Exception(ErrorCodes::UNSUPPORTED, "Range join keyed stream or changelog stream is not supported");

        return;
    }

    /// We don't even care the left stream semantic since if right stream is versioned-kv or changelog-kv
    /// we will use `Multiple` list to hold the values since users may use partial primary key to join
    if (!isAppendDataStream(right_join_ctx.join_stream_desc->data_stream_semantic))
        streaming_strictness = Streaming::Strictness::Multiple;
}

std::optional<HashJoin::AsofJoinKeyColumnInfo> HashJoin::doValidateAsofJoinKey() const
{
    if (streaming_strictness != Strictness::Range)
        return {};

    if (table_join->rangeAsofJoinContext().type != RangeType::Interval)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Only time interval range join is supported in stream to stream join. Use `date_diff(...) or date_diff_within(...) function.");

    const auto & join_clause = table_join->getOnlyClause();

    auto check_type = [](const Block & header, size_t col_pos) {
        auto type = header.getByPosition(col_pos).type;
        if (!isDateTime64(type) && !isDateTime(type))
            throw DB::Exception(
                ErrorCodes::NOT_IMPLEMENTED, "The range column in stream to stream join only supports datetime64 or datetime column types");
    };

    auto left_asof_col_pos = left_join_ctx.join_stream_desc->input_header.getPositionByName(join_clause.key_names_left.back());
    auto right_asof_col_pos = right_join_ctx.join_stream_desc->input_header.getPositionByName(join_clause.key_names_right.back());

    check_type(left_join_ctx.join_stream_desc->input_header, left_asof_col_pos);
    check_type(right_join_ctx.join_stream_desc->input_header, right_asof_col_pos);

    const auto & left_asof_col_with_type = left_join_ctx.join_stream_desc->input_header.getByPosition(left_asof_col_pos);
    const auto & right_asof_col_with_type = right_join_ctx.join_stream_desc->input_header.getByPosition(right_asof_col_pos);

    if (left_asof_col_with_type.type->getTypeId() != right_asof_col_with_type.type->getTypeId())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The columns in stream to stream range join have different column types");

    UInt32 scale = 0;
    if (isDateTime64(left_asof_col_with_type.type))
    {
        /// Check scale
        const auto * left_datetime64_type = checkAndGetDataType<DataTypeDateTime64>(left_asof_col_with_type.type.get());
        assert(left_datetime64_type);

        const auto * right_datetime64_type = checkAndGetDataType<DataTypeDateTime64>(right_asof_col_with_type.type.get());
        assert(right_datetime64_type);

        if (left_datetime64_type->getScale() != right_datetime64_type->getScale())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The datetime64 columns in stream to stream range join have different scales");

        scale = left_datetime64_type->getScale();
    }

    return AsofJoinKeyColumnInfo{
        .scale = scale,
        .type_index = left_asof_col_with_type.type->getTypeId(),
        .left_col_pos = left_asof_col_pos,
        .right_col_pos = right_asof_col_pos,
    };
}

bool HashJoin::isRetractBlock(const Block & block, const JoinStreamDescription & join_stream_desc)
{
    if (!join_stream_desc.hasDeltaColumn())
        return false;

    assert(!Streaming::isAppendDataStream(join_stream_desc.data_stream_semantic));
    const auto & delta_column = block.getByPosition(*join_stream_desc.delta_column_position);
    return delta_column.column->getInt(0) < 0;
}

template <bool is_left_block>
void HashJoin::addMissingColumns(Block & block) const
{
    if (!block.rows())
        return;

    const auto & on_exprs = table_join->getClauses();
    std::vector<JoinOnKeyColumns> join_on_keys;
    join_on_keys.reserve(on_exprs.size());
    for (size_t i = 0; i < on_exprs.size(); ++i)
    {
        if constexpr (is_left_block)
            join_on_keys.emplace_back(block, on_exprs[i].key_names_left, cond_column_names[i].first, key_sizes[i]);
        else
            join_on_keys.emplace_back(block, on_exprs[i].key_names_right, cond_column_names[i].second, key_sizes[i]);
    }

    const Block * joined_sample_block;
    const JoinContext * joined_ctx;
    if constexpr (is_left_block)
    {
        joined_sample_block = &savedRightBlockSample();
        joined_ctx = &right_join_ctx;
    }
    else
    {
        joined_sample_block = &savedLeftBlockSample();
        joined_ctx = &left_join_ctx;
    }

    bool is_asof_join = (streaming_strictness == Strictness::Asof || streaming_strictness == Strictness::Range);
    AddedColumns added_columns(
        block,
        joined_ctx->sample_block_with_columns_to_add,
        *joined_sample_block,
        *this,
        std::move(join_on_keys),
        table_join->rangeAsofJoinContext(),
        is_asof_join,
        is_left_block);

    added_columns.appendDefaultRows(added_columns.rows_to_add);
    added_columns.applyLazyDefaults();

    for (size_t i = 0; i < added_columns.size(); ++i)
        block.insert(added_columns.moveColumn(i));

    /// Add joined key columns from joined block if needed.
    for (size_t i = 0, columns = joined_ctx->required_keys.columns(); i < columns; ++i)
    {
        const auto & joined_key = joined_ctx->required_keys.getByPosition(i);
        auto joined_col_name = is_left_block ? getTableJoin().renamedRightColumnName(joined_key.name) : joined_key.name;
        if (!block.findByName(joined_col_name))
        {
            /// asof column is already in block.
            const String * joined_key_name_in_clause;
            if constexpr (is_left_block)
                joined_key_name_in_clause = &table_join->getOnlyClause().key_names_right.back();
            else
                joined_key_name_in_clause = &table_join->getOnlyClause().key_names_left.back();

            if (is_asof_join && joined_key.name == *joined_key_name_in_clause)
                continue;

            const auto & col = block.getByName(joined_ctx->required_keys_sources[i]);
            auto joined_key_col = copyLeftKeyColumnToRight(joined_key.type, joined_col_name, col);
            block.insert(std::move(joined_key_col));
        }
    }
}

void HashJoin::JoinGlobalMetrics::serialize(WriteBuffer & wb, VersionType) const
{
    DB::writeBinary(total_join, wb);
    DB::writeBinary(left_block_and_right_range_bucket_no_intersection_skip, wb);
    DB::writeBinary(right_block_and_left_range_bucket_no_intersection_skip, wb);
}

void HashJoin::JoinGlobalMetrics::deserialize(ReadBuffer & rb, VersionType)
{
    DB::readBinary(total_join, rb);
    DB::readBinary(left_block_and_right_range_bucket_no_intersection_skip, rb);
    DB::readBinary(right_block_and_left_range_bucket_no_intersection_skip, rb);
}

/// Explicit instantiation of template functions
template void HashJoin::addMissingColumns<true>(Block & block) const;
template void HashJoin::addMissingColumns<false>(Block & block) const;
}

}
