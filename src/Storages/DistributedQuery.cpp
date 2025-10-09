#include <Storages/DistributedQuery.h>
#include <Storages/IStorage.h>
#include <Storages/PruneShards.h>
#include <Storages/SelectQueryInfo.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterUtil.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

namespace
{
/// constexpr UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY = 1;
/// constexpr UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS = 2;
constexpr UInt64 DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION = 2;
/// constexpr UInt64 PARALLEL_DISTRIBUTED_INSERT_SELECT_ALL = 2;

String makeFormattedListOfShards(const ClusterPtr & cluster)
{
    WriteBufferFromOwnString buf;

    bool head = true;
    buf << "[";
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        (head ? buf : buf << ", ") << shard_info.shard_num;
        head = false;
    }
    buf << "]";

    return buf.str();
}

size_t getClusterQueriedNodes(const Settings & settings, const ClusterPtr & cluster)
{
    size_t num_local_shards = cluster->getLocalShardCount();
    size_t num_remote_shards = cluster->getRemoteShardCount();
    return (num_remote_shards * settings.max_parallel_replicas) + num_local_shards;
}

std::optional<QueryProcessingStage::Enum> getOptimizedHistoricalQueryProcessingStage(
    const ExpressionActionsPtr & sharding_key_expr,
    bool sharding_key_is_deterministic,
    const SelectQueryInfo & query_info,
    const Settings & settings)
{
    bool optimize_sharding_key_aggregation = settings.optimize_skip_unused_shards && settings.optimize_distributed_group_by_sharding_key
        && (settings.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic);

    QueryProcessingStage::Enum default_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;
    if (settings.distributed_push_down_limit)
        default_stage = QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;

    const auto & select = query_info.query->as<ASTSelectQuery &>();

    auto expr_contains_sharding_key = [&](const auto & exprs) -> bool {
        std::unordered_set<std::string> expr_columns;
        for (auto & expr : exprs)
        {
            auto id = expr->template as<ASTIdentifier>();
            if (!id)
                continue;
            expr_columns.emplace(id->name());
        }

        for (const auto & column : sharding_key_expr->getRequiredColumns())
        {
            if (!expr_columns.contains(column))
                return false;
        }

        return true;
    };

    /// GROUP BY qualifiers
    /// - TODO: WITH TOTALS can be implemented
    /// - TODO: WITH ROLLUP can be implemented (I guess)
    if (select.group_by_with_totals || select.group_by_with_rollup || select.group_by_with_cube)
        return {};

    /// Window functions are not supported.
    if (query_info.has_window)
        return {};
    /// TODO: extremes support can be implemented
    if (settings.extremes)
        return {};

    /// DISTINCT
    if (select.distinct)
    {
        if (!optimize_sharding_key_aggregation || !expr_contains_sharding_key(select.select()->children))
            return {};
    }

    /// GROUP BY
    const ASTPtr group_by = select.groupBy();

    bool has_aggregates = query_info.has_aggregates;
    if (query_info.syntax_analyzer_result)
        has_aggregates = !query_info.syntax_analyzer_result->aggregates.empty();

    if (has_aggregates || group_by)
    {
        if (!optimize_sharding_key_aggregation || !group_by || !expr_contains_sharding_key(group_by->children))
            return {};
    }

    /// LIMIT BY
    if (const ASTPtr limit_by = select.limitBy())
    {
        if (!optimize_sharding_key_aggregation || !expr_contains_sharding_key(limit_by->children))
            return {};
    }

    /// ORDER BY
    if (const ASTPtr order_by = select.orderBy())
        return default_stage;

    /// LIMIT
    /// OFFSET
    if (select.limitLength() || select.limitOffset())
        return default_stage;

    /// Only simple SELECT FROM GROUP BY sharding_key can use Complete state.
    return QueryProcessingStage::Complete;
}

}

ExpressionActionsPtr
buildShardingKeyExpression(const ASTPtr & sharding_key, ContextPtr context, const NamesAndTypesList & columns, bool project)
{
    ASTPtr query = sharding_key;
    auto syntax_result = TreeRewriter(context).analyze(query, columns);
    return ExpressionAnalyzer(query, syntax_result, context).getActions(project);
}

bool isExpressionActionsDeterministics(const ExpressionActionsPtr & actions)
{
    for (const auto & action : actions->getActions())
    {
        if (action.node->type != ActionsDAG::ActionType::FUNCTION)
            continue;

        if (!action.node->function_base->isDeterministic())
            return false;
    }
    return true;
}

QueryProcessingStage::Enum getHistoricalQueryProcessingStageRemote(
    SelectQueryInfo & query_info,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr local_context,
    ConstStoragePtr storage,
    const ExpressionActionsPtr & sharding_key_expr,
    bool sharding_key_is_deterministic,
    const String & sharding_key_column_name,
    const std::vector<UInt64> & all_shards,
    LoggerPtr logger)
{
    /// return nullptr since we don't have distributed queries
    ClusterPtr cluster = nullptr;
    query_info.cluster = cluster;

    auto pruned_shards = pruneShards(
        sharding_key_expr,
        sharding_key_is_deterministic,
        sharding_key_column_name,
        storage,
        storage_snapshot,
        query_info,
        all_shards,
        local_context,
        logger);

    if (pruned_shards.size() != all_shards.size())
    {
        cluster = cluster->getClusterWithMultipleShards(pruned_shards);
        query_info.optimized_cluster = cluster;

        LOG_INFO(
            logger,
            "Skipping irrelevant shards - the query will be sent to the following shards of the cluster (shard numbers): {}",
            makeFormattedListOfShards(cluster));
    }
    else
    {
        LOG_DEBUG(
            logger,
            "Unable to figure out irrelevant shards from WHERE/PREWHERE clauses - the query will be sent to all shards of the cluster");
    }

    const auto & settings = local_context->getSettingsRef();
    if (settings.distributed_group_by_no_merge)
    {
        if (settings.distributed_group_by_no_merge == DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION)
        {
            if (settings.distributed_push_down_limit)
                return QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
            return QueryProcessingStage::WithMergeableStateAfterAggregation;
        }

        /// NOTE: distributed_group_by_no_merge=1 does not respect distributed_push_down_limit
        /// (since in this case queries processed separately and the initiator is just a proxy in this case).
        if (to_stage != QueryProcessingStage::Complete)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Queries with distributed_group_by_no_merge=1 should be processed to Complete stage");

        return QueryProcessingStage::Complete;
    }

    /// Nested distributed query cannot return Complete stage,
    /// since the parent query need to aggregate the results after.
    if (to_stage == QueryProcessingStage::WithMergeableState)
        return QueryProcessingStage::FetchColumns;

    auto nodes = getClusterQueriedNodes(settings, cluster);

    /// If there is only one node, the query can be fully processed by the
    /// shard, initiator will work as a proxy only.
    if (nodes == 1)
    {
        /// In case the query was processed to
        /// WithMergeableStateAfterAggregation/WithMergeableStateAfterAggregationAndLimit
        /// (which are greater the Complete stage)
        /// we cannot return Complete (will break aliases and similar),
        /// relevant for distributed query over distributed query
        return std::max(to_stage, QueryProcessingStage::Complete);
    }

    if (nodes == 0)
    {
        /// In case of 0 shards, the query should be processed fully on the initiator,
        /// since we need to apply aggregations.
        /// That's why we need to return FetchColumns.
        return QueryProcessingStage::FetchColumns;
    }

    auto optimized_stage
        = getOptimizedHistoricalQueryProcessingStage(sharding_key_expr, sharding_key_is_deterministic, query_info, settings);
    if (optimized_stage)
    {
        if (*optimized_stage == QueryProcessingStage::Complete)
            return std::min(to_stage, *optimized_stage);
        return *optimized_stage;
    }

    return QueryProcessingStage::WithMergeableState;
}

}
