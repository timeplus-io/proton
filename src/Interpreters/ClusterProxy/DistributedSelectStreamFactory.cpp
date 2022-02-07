#include "DistributedSelectStreamFactory.h"

/// #include <DataStreams/RemoteQueryExecutor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/checkStackSize.h>


namespace DB
{
namespace ClusterProxy
{
namespace
{

ActionsDAGPtr getConvertingDAG(const Block & block, const Block & header)
{
    /// Convert header structure to expected.
    /// Also we ignore constants from result and replace it with constants from header.
    /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
    return ActionsDAG::makeConvertingActions(
        block.getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        true);
}

void addConvertingActions(QueryPlan & plan, const Block & header)
{
    if (blocksHaveEqualStructure(plan.getCurrentDataStream().header, header))
        return;

    auto convert_actions_dag = getConvertingDAG(plan.getCurrentDataStream().header, header);
    auto converting = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), convert_actions_dag);
    plan.addStep(std::move(converting));
}

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    UInt32 shard_num,
    UInt32 shard_count)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();

    InterpreterSelectQuery interpreter(query_ast, context, SelectQueryOptions(processed_stage).setShardInfo(shard_num, shard_count));
    interpreter.buildQueryPlan(*query_plan);

    addConvertingActions(*query_plan, header);

    return query_plan;
}
}

DistributedSelectStreamFactory::DistributedSelectStreamFactory(
    const Block & header_,
    QueryProcessingStage::Enum processed_stage_,
    bool has_virtual_shard_num_column_)
    : header(header_)
    , processed_stage{processed_stage_}
    , has_virtual_shard_num_column(has_virtual_shard_num_column_)
{
}

void DistributedSelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const ASTPtr & query_ast,
    const StorageID & /* main_table */,
    const ASTPtr & /* table_func_ptr */,
    ContextPtr context,
    std::vector<QueryPlanPtr> & local_plans,
    Shards & remote_shards,
    UInt32 shard_count)
{
    auto modified_query_ast = query_ast->clone();
    if (has_virtual_shard_num_column)
        VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, "_shard_num", shard_info.shard_num, "to_uint32");

    auto emplace_local_stream = [&]()
    {
        /// HACKY : Setup query_kind to break infinite query forwarding loop
        auto mutable_context = std::const_pointer_cast<Context>(context);
        auto & client_info = mutable_context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

        local_plans.emplace_back(createLocalPlan(modified_query_ast, header, context, processed_stage, shard_info.shard_num, shard_count));
    };

    auto emplace_remote_stream = [&](bool lazy = false, UInt32 local_delay = 0)
    {
        remote_shards.emplace_back(Shard{
            .query = modified_query_ast,
            .header = header,
            .shard_num = shard_info.shard_num,
            .pool = shard_info.pool,
            .lazy = lazy,
            .local_delay = local_delay,
        });
    };

    const auto & settings = context->getSettingsRef();

    if (settings.prefer_localhost_replica && shard_info.isLocal())
    {
        /// FIXME : staleness checking
        emplace_local_stream();
    }
    else
    {
        emplace_remote_stream();
    }
}
}
}
