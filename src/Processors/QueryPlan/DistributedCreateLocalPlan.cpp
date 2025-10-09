#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/QueryPlan/ConvertingActions.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Common/checkStackSize.h>

namespace DB
{

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t shard_num,
    size_t shard_count,
    bool has_missing_objects,
    bool is_streaming)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();
    auto new_context = Context::createCopy(context);
    new_context->setSetting("query_mode", is_streaming ? String{"streaming"} : String{"table"});
    /// To avoid infinite recursive query
    new_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    /// Do not push down limit to local plan, as it will break `rows_before_limit_at_least` counter.
    if (processed_stage == QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit)
        processed_stage = QueryProcessingStage::WithMergeableStateAfterAggregation;

    /// Do not apply AST optimizations, because query
    /// is already optimized and some optimizations
    /// can be applied only for non-distributed tables
    /// and we can produce query, inconsistent with remote plans.
    auto select_query_options = SelectQueryOptions(processed_stage)
                                    .setShardInfo(static_cast<UInt32>(shard_num), static_cast<UInt32>(shard_count))
                                    .ignoreASTOptimizations();

    {
        InterpreterSelectQuery interpreter(query_ast, new_context, select_query_options);
        interpreter.buildQueryPlan(*query_plan);
    }

    addConvertingActions(*query_plan, header, has_missing_objects);
    return query_plan;
}

}
