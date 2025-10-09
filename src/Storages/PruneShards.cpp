#include <Storages/PruneShards.h>

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/parseShards.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_MANY_ROWS;
extern const int TYPE_MISMATCH;
}

IColumn::Selector createSelector(const ColumnWithTypeAndName & result, const std::vector<UInt64> & slot_to_shards)
{
/// NOLINTBEGIN(readability-else-after-return)
/// If result.type is DataTypeLowCardinality, do shard according to its dictionaryType
#define CREATE_FOR_TYPE(TYPE) \
    if (typeid_cast<const DataType##TYPE *>(result.type.get())) \
        return createBlockSelector<TYPE>(*result.column, slot_to_shards); \
    else if (auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(result.type.get())) \
        if (typeid_cast<const DataType##TYPE *>(type_low_cardinality->getDictionaryType().get())) \
            return createBlockSelector<TYPE>(*result.column->convertToFullColumnIfLowCardinality(), slot_to_shards);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{ErrorCodes::TYPE_MISMATCH, "Selector column is not an integer type"};
}
/// NOLINTEND(readability-else-after-return)


namespace
{
class ReplacingConstantExpressionsMatcher
{
public:
    using Data = Block;

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & node, Block & block_with_constants)
    {
        if (!node->as<ASTFunction>())
            return;

        std::string name = node->getColumnName();
        if (block_with_constants.has(name))
        {
            auto result = block_with_constants.getByName(name);
            if (!isColumnConst(*result.column))
                return;

            node = std::make_shared<ASTLiteral>(assert_cast<const ColumnConst &>(*result.column).getField());
        }
    }
};

void replaceConstantExpressions(
    ASTPtr & node,
    ContextPtr context,
    const NamesAndTypesList & columns,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot)
{
    auto syntax_result = TreeRewriter(context).analyze(node, columns, storage, storage_snapshot);
    Block block_with_constants = KeyCondition::getBlockWithConstants(node, syntax_result, context);

    InDepthNodeVisitor<ReplacingConstantExpressionsMatcher, true> visitor(block_with_constants);
    visitor.visit(node);
}

/// Returns a pruned shard IDs (fewer shards) if constant folding for `sharding_key_expr` is possible
/// using constraints from "PREWHERE" and "WHERE" conditions, otherwise returns all_shards
std::vector<UInt64> skipUnusedShards(
    const ExpressionActionsPtr & sharding_key_expr,
    const String & sharding_key_column_name,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const std::vector<UInt64> & all_shards,
    const ContextPtr & context,
    LoggerPtr logger)
{
    const auto & query_ptr = query_info.query;
    const auto & select = query_ptr->as<ASTSelectQuery &>();
    if (!select.prewhere() && !select.where())
        return all_shards;

    if (!query_info.syntax_analyzer_result)
        return all_shards;

    ASTPtr condition_ast;
    /// Remove JOIN from the query since it may contain a condition for other tables.
    /// But only the conditions for the left table should be analyzed for shard skipping.
    {
        ASTPtr select_without_join_ptr = select.clone();
        ASTSelectQuery select_without_join = select_without_join_ptr->as<ASTSelectQuery &>();
        TreeRewriterResult analyzer_result_without_join = *query_info.syntax_analyzer_result;

        removeJoin(select_without_join, analyzer_result_without_join, context);
        if (!select_without_join.prewhere() && !select_without_join.where())
            return all_shards;

        if (select.prewhere() && select.where())
            condition_ast = makeASTFunction("and", select.prewhere()->clone(), select.where()->clone());
        else
            condition_ast = select.prewhere() ? select.prewhere()->clone() : select.where()->clone();
    }

    replaceConstantExpressions(condition_ast, context, storage_snapshot->metadata->getColumns().getAll(), storage, storage_snapshot);

    size_t limit = context->getSettingsRef().optimize_skip_unused_shards_limit;
    if (!limit || limit > LONG_MAX)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "optimize_skip_unused_shards_limit out of range (0, {}]", LONG_MAX);

    /// To interpret limit==0 as limit is reached
    ++limit;
    const auto blocks = evaluateExpressionOverConstantCondition(condition_ast, sharding_key_expr, limit);

    if (!limit)
    {
        LOG_DEBUG(
            logger,
            "Number of values for sharding key exceeds optimize_skip_unused_shards_limit={}, "
            "try to increase it, but note that this may increase query processing time.",
            context->getSettingsRef().optimize_skip_unused_shards_limit);

        return all_shards;
    }

    /// Can't get definite answer if we can skip any shards
    if (!blocks)
        return all_shards;

    std::set<Int64> shard_ids;

    for (const auto & block : *blocks)
    {
        if (!block.has(sharding_key_column_name))
            throw Exception(ErrorCodes::TOO_MANY_ROWS, "sharding_key_expr should evaluate as a single row");

        const ColumnWithTypeAndName & result = block.getByName(sharding_key_column_name);
        const auto selector = createSelector(result, all_shards);

        shard_ids.insert(selector.begin(), selector.end());
    }

    return std::vector<UInt64>{shard_ids.begin(), shard_ids.end()};
}

std::vector<UInt64> getQueryShards(
    const ExpressionActionsPtr & sharding_key_expr,
    bool sharding_key_is_deterministic,
    const String & sharding_key_column_name,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const std::vector<UInt64> & all_shards,
    const ContextPtr & context,
    LoggerPtr logger)
{
    /// Special case: read specified shard
    /// There are 2 cases so far we may read on specific shard or shards
    /// 1) SELECT * FROM stream SETTINGS shards='0,2';
    /// 2) Distributed query on a local shard in SelectQueryOptions::shard_num
    if (auto only_one_shard_to_read = context->getShardToRead(); only_one_shard_to_read.has_value())
    {
        chassert(all_shards.size() > static_cast<size_t>(only_one_shard_to_read.value()));
        return std::vector<UInt64>{static_cast<UInt64>(only_one_shard_to_read.value())};
    }
    else
    {
        const auto & shards_setting = context->getSettingsRef().shards.value;
        if (!shards_setting.empty())
            return parseQueryShards(shards_setting, all_shards.size());
        else
            return pruneShards(
                sharding_key_expr,
                sharding_key_is_deterministic,
                sharding_key_column_name,
                std::move(storage),
                storage_snapshot,
                query_info,
                all_shards,
                context,
                logger);
    }
}

}

std::vector<UInt64> pruneShards(
    const ExpressionActionsPtr & sharding_key_expr,
    bool sharding_key_is_deterministic,
    const String & sharding_key_column_name,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const std::vector<UInt64> & all_shards,
    const ContextPtr & context,
    LoggerPtr logger)
{
    if (all_shards.size() == 1)
        return all_shards;

    const auto & settings_ref = context->getSettingsRef();
    if (!settings_ref.optimize_skip_unused_shards)
        return all_shards;

    bool sharding_key_is_usable = settings_ref.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic;
    if (!sharding_key_is_usable)
        return all_shards;

    return skipUnusedShards(
        sharding_key_expr, sharding_key_column_name, std::move(storage), storage_snapshot, query_info, all_shards, context, logger);
}

QueryMode getQueryMode(ConstStoragePtr storage, const SelectQueryInfo & query_info, const ContextPtr & context)
{
    if (query_info.isStreaming())
    {
        chassert(query_info.seek_to_info);
        const auto & settings_ref = context->getSettingsRef();

        /// We allow backfill from historical store the following scenarios for non-recovered streaming query:
        /// 1) For non-inmemory keyed storage stream, we always back fill from historical store (e.g. VersionedKV, ChangelogKV)
        /// 2) Do time travel with settings `enable_backfill_from_historical_store = true`
        bool require_back_fill_from_historical = false;
        if (!storage->isInmemory() && settings_ref.exec_mode != ExecuteMode::Recover
            && (Streaming::isKeyedStorage(storage->dataStreamSemantic()) || !query_info.seek_to_info->getSeekTo().empty()))
            require_back_fill_from_historical = settings_ref.enable_backfill_from_historical_store.value;

        if (require_back_fill_from_historical)
        {
            /// By default, we will seek to earliest for backfill concat
            if (query_info.seek_to_info->getSeekTo().empty())
                query_info.seek_to_info->seek_points = {cluster::Constants::EarliestSN};

            return QueryMode::StreamingConcat;
        }
        else
        {
            return QueryMode::Streaming;
        }
    }
    else
    {
        return QueryMode::Historical;
    }
}

ShardsWithQueryMode getPrunedShardsWithQueryMode(
    const ExpressionActionsPtr & sharding_key_expr,
    bool sharding_key_is_deterministic,
    const String & sharding_key_column_name,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const std::vector<UInt64> & all_shards,
    const ContextPtr & context,
    LoggerPtr logger)
{
    ShardsWithQueryMode result;
    result.mode = getQueryMode(storage, query_info, context);
    result.shards = getQueryShards(
        sharding_key_expr,
        sharding_key_is_deterministic,
        sharding_key_column_name,
        std::move(storage),
        storage_snapshot,
        query_info,
        all_shards,
        context,
        logger);

    return result;
}

}
