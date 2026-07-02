#include <Storages/PruneShards.h>
#include <Storages/PruneShardsDetail.h>

#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/interpretSubquery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/parseShards.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int NOT_IMPLEMENTED;
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

bool isSubqueryPlaceholder(const ASTPtr & node)
{
    return node && (node->as<ASTSubquery>() || node->as<ASTTableIdentifier>());
}

}

namespace detail
{

bool canMaterializeSubqueryForShardPruning(const QueryPlan & subquery_plan)
{
    return !subquery_plan.isStreaming();
}

std::unique_ptr<QueryPlan> buildSubqueryPlanForShardPruning(
    const ASTPtr & subquery, const ContextPtr & context, size_t subquery_depth)
{
    auto subquery_plan = std::make_unique<QueryPlan>();
    auto interpreter = interpretSubquery(subquery, context, subquery_depth, {});
    interpreter->buildQueryPlan(*subquery_plan);
    return subquery_plan;
}

FutureSetPtr addSubqueryPlanForShardPruning(
    const PreparedSetsPtr & prepared_sets,
    const PreparedSets::Hash & set_key,
    std::unique_ptr<QueryPlan> subquery_plan,
    const ContextPtr & context,
    size_t limit)
{
    if (!canMaterializeSubqueryForShardPruning(*subquery_plan))
        return nullptr;

    /// Shard pruning only needs enough explicit values to decide whether
    /// the set exceeds its own limit. Keep normal set semantics intact,
    /// but do not collect an unbounded explicit Field list for planning.
    auto pruning_settings = context->getSettingsRef();
    const auto pruning_max_values = limit == std::numeric_limits<UInt64>::max()
        ? std::numeric_limits<UInt64>::max()
        : static_cast<UInt64>(limit) + 1;
    if (!pruning_settings.use_index_for_in_with_subqueries_max_values
        || pruning_settings.use_index_for_in_with_subqueries_max_values > pruning_max_values)
        pruning_settings.use_index_for_in_with_subqueries_max_values = pruning_max_values;

    return prepared_sets->addFromSubquery(
        set_key,
        std::move(subquery_plan),
        /*external_table=*/nullptr,
        /*external_table_set=*/nullptr,
        pruning_settings);
}

bool rewriteInSubqueriesForShardPruning(
    ASTPtr & node,
    const PreparedSetsPtr & prepared_sets,
    const ContextPtr & context,
    size_t subquery_depth,
    size_t limit,
    RewriteInSubqueriesForShardPruningResult & result,
    bool in_conjunctive_position)
{
    if (!node)
        return false;

    /// Do not recurse into subquery bodies. Only the outer IN expression matters here.
    if (node->as<ASTSubquery>() || node->as<ASTTableIdentifier>())
        return false;

    bool changed = false;

    const auto * function = node->as<ASTFunction>();

    /// Only `and(...)` lets an empty IN's "always false" value reach the root predicate.
    /// Every other boolean-consuming operator -- `or`, `not`, `not_in`, `if`, `multi_if`,
    /// `coalesce`, `tuple`, comparisons, arithmetic, arbitrary user functions -- may suppress
    /// that signal, so descending through them moves us out of conjunctive position. Without
    /// this guard `WHERE if(flag, id IN (SELECT 1 WHERE 0), 1)` would prune every shard even
    /// though `flag = 0` rows must still be returned.
    /// Non-function nodes (notably `ASTExpressionList`, which wraps a function's arguments)
    /// are transparent: they do not change conjunctive position by themselves.
    bool children_in_conjunctive_position = in_conjunctive_position;
    if (function && function->name != "and")
        children_in_conjunctive_position = false;

    /// Traverse each branch independently so multiple IN-subqueries can be rewritten one by one.
    for (auto & child : node->children)
        changed |= rewriteInSubqueriesForShardPruning(
            child, prepared_sets, context, subquery_depth, limit, result, children_in_conjunctive_position);

    if (!function || function->name != "in" || !function->arguments || function->arguments->children.size() != 2)
        return changed;

    ASTPtr & right = function->arguments->children[1];
    if (!isSubqueryPlaceholder(right))
        return changed;

    if (!prepared_sets)
        return changed;

    /// Shard pruning runs in `IStorage::getQueryProcessingStage`, which is invoked before
    /// `ExpressionAnalysisResult` runs `ActionsVisitor` over WHERE/PREWHERE. That means
    /// `prepared_sets` has not yet seen the IN-subqueries at this point. We first try to
    /// reuse whatever was already registered (e.g. by an earlier visitor pass), and if
    /// nothing matches we register and materialize the subquery ourselves -- this is what
    /// the `optimize_skip_unused_shards_with_subqueries` setting opts into.
    const auto right_hash = right->getTreeHash();
    FutureSetPtr future_set = prepared_sets->findSubquery(right_hash);
    if (!future_set && right->as<ASTTableIdentifier>())
        future_set = prepared_sets->findStorage(right_hash);

    if (!future_set && right->as<ASTSubquery>() && context->getSettingsRef().use_index_for_in_with_subqueries)
    {
        try
        {
            auto subquery_plan = buildSubqueryPlanForShardPruning(right, context, subquery_depth);

            /// A streaming subquery cannot be materialized into a fixed set -- the upstream
            /// `buildOrderedSetInplace()` would throw `NOT_IMPLEMENTED`. Detect that here so
            /// we never register a future set we'd immediately fail to consume; the original
            /// AST is left in place and normal execution handles the IN.
            future_set = addSubqueryPlanForShardPruning(prepared_sets, right_hash, std::move(subquery_plan), context, limit);
            if (!future_set)
                return changed;
        }
        catch (const DB::Exception &)
        {
            /// Analyzer failures and unsupported subquery shapes use DB::Exception.
            /// Non-DB exceptions are runtime/programmer bugs and should propagate.
            return changed;
        }
    }

    if (!future_set)
        return changed;

    SetPtr set;
    try
    {
        set = future_set->buildOrderedSetInplace(context);
    }
    catch (const DB::Exception & e)
    {
        if (e.code() == ErrorCodes::NOT_IMPLEMENTED)
            return changed;
        throw;
    }

    if (!set || !set->hasExplicitSetElements())
        return changed;

    const size_t total_rows = set->getTotalRowCount();
    if (total_rows == 0)
    {
        /// An empty IN-subquery is always false. Only flag it for shard short-circuiting when
        /// it sits in a pure conjunctive position -- otherwise an enclosing operator may still
        /// evaluate the whole predicate to true for some rows (e.g. `if(flag, id IN (empty), 1)`).
        if (in_conjunctive_position)
            result.has_empty_subquery_in_conjunctive_position = true;
        return changed;
    }

    if (total_rows > limit)
        return changed;

    const auto elements = set->getSetElements();
    if (elements.size() != 1)
        return changed;

    const auto & values = elements.front();
    if (!values || values->size() == 0)
    {
        if (in_conjunctive_position)
            result.has_empty_subquery_in_conjunctive_position = true;
        return changed;
    }

    Tuple tuple;
    tuple.reserve(values->size());

    for (size_t row = 0; row < values->size(); ++row)
    {
        Field value = (*values)[row];
        if (value.isNull())
        {
            /// Any NULL element forces a conservative fallback: with transform_null_in=1
            /// or a nullable sharding key, dropping NULLs from the rewrite would prune
            /// shards that actually hold NULL-keyed rows. Keep the original subquery so
            /// shard pruning gives up and reads every shard — correct, just unoptimized.
            return changed;
        }

        tuple.push_back(std::move(value));
    }

    /// `values->size() == 0` returns above, and any NULL element returns from inside the loop.
    /// So if we reach here, the loop ran at least once and pushed at least one element.
    chassert(!tuple.empty());

    /// Replace the subquery with a literal tuple so evaluateExpressionOverConstantCondition()
    /// sees the concrete key set just like it does for literal IN-lists.
    right = std::make_shared<ASTLiteral>(std::move(tuple));
    return true;
}

}

namespace
{

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

    /// Streaming queries must never return an empty shard list: that would create
    /// a finite empty source and terminate a continuous query. Both zero-shard
    /// exits below preserve that invariant.
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

    size_t max_shard_key_values = context->getSettingsRef().optimize_skip_unused_shards_limit;
    if (!max_shard_key_values || max_shard_key_values > LONG_MAX)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "optimize_skip_unused_shards_limit out of range (0, {}]", LONG_MAX);

    detail::RewriteInSubqueriesForShardPruningResult rewrite_result;
    if (context->getSettingsRef().optimize_skip_unused_shards_with_subqueries)
    {
        detail::rewriteInSubqueriesForShardPruning(
            condition_ast,
            query_info.prepared_sets,
            context,
            query_info.syntax_analyzer_result->subquery_depth,
            max_shard_key_values,
            rewrite_result,
            /*in_conjunctive_position=*/true);

        if (rewrite_result.has_empty_subquery_in_conjunctive_position && !query_info.isStreaming())
        {
            /// An empty IN-subquery reached purely through `and(...)` ancestors contradicts the
            /// whole predicate, so no shard can produce a matching row.
            return {};
        }
    }

    replaceConstantExpressions(condition_ast, context, storage_snapshot->metadata->getColumns().getAll(), storage, storage_snapshot);

    size_t limit = max_shard_key_values;

    /// To interpret limit==0 as limit is reached
    ++limit;
    const auto blocks = evaluateExpressionOverConstantCondition(condition_ast, sharding_key_expr, limit);

    if (!limit)
    {
        LOG_DEBUG(
            logger,
            "Number of values for sharding key exceeds optimize_skip_unused_shards_limit={}, "
            "try to increase it, but note that this may increase query processing time.",
            context->getSettingsRef().optimize_skip_unused_shards_limit.value);

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

    /// A zero-shard result is a finite empty read. That is correct for historical
    /// queries, but a streaming query must remain continuous even when its predicate
    /// is currently/provably unsatisfiable. Fall back to all shards so the normal
    /// streaming filter path preserves query lifetime.
    if (shard_ids.empty() && query_info.isStreaming())
        return all_shards;

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
