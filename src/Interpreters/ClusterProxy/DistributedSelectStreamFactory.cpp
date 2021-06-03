#include "DistributedSelectStreamFactory.h"

#include <DataStreams/RemoteQueryExecutor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Processors/Pipe.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
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

void addConvertingActions(Pipe & pipe, const Block & header)
{
    if (blocksHaveEqualStructure(pipe.getHeader(), header))
        return;

    auto convert_actions = std::make_shared<ExpressionActions>(getConvertingDAG(pipe.getHeader(), header));
    pipe.addSimpleTransform([&](const Block & cur_header, Pipe::StreamType) -> ProcessorPtr
    {
        return std::make_shared<ExpressionTransform>(cur_header, convert_actions);
    });
}

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();

    InterpreterSelectQuery interpreter(query_ast, context, SelectQueryOptions(processed_stage));
    interpreter.buildQueryPlan(*query_plan);

    addConvertingActions(*query_plan, header);

    return query_plan;
}

String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return {};

    WriteBufferFromOwnString buf;
    formatAST(*ast, buf, false, true);
    return buf.str();
}
}

DistributedSelectStreamFactory::DistributedSelectStreamFactory(
    const Block & header_,
    QueryProcessingStage::Enum processed_stage_,
    StorageID main_table_,
    const Scalars & scalars_,
    const Tables & external_tables_)
    : header(header_)
    , processed_stage{processed_stage_}
    , main_table(std::move(main_table_))
    , scalars{scalars_}
    , external_tables{external_tables_}
{
}

void DistributedSelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const ASTPtr & query_ast,
    ContextPtr context,
    const ThrottlerPtr & throttler,
    const SelectQueryInfo &,
    std::vector<QueryPlanPtr> & plans,
    Pipes & remote_pipes,
    Pipes & /* delayed_pipes */,
    Poco::Logger * log)
{
    bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context->getSettingsRef().async_socket_for_remote;

    if (processed_stage == QueryProcessingStage::Complete)
    {
        add_totals = query_ast->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context->getSettingsRef().extremes;
    }

    auto emplace_local_stream = [&]() { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        /// HACKY : Setup query_kind to break infinit query forwarding loop
        auto & client_info = context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

        plans.emplace_back(createLocalPlan(query_ast, header, context, processed_stage));
        addConvertingActions(*plans.back(), header);
    };

    String query = formattedAST(query_ast);

    auto emplace_remote_stream = [&]() { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            shard_info.pool, query, header, context, throttler, scalars, external_tables, processed_stage);
        remote_query_executor->setLogger(log);

        remote_query_executor->setPoolMode(PoolMode::GET_MANY);
        remote_query_executor->setMainTable(main_table);

        remote_pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read));
        remote_pipes.back().addInterpreterContext(context);
        addConvertingActions(remote_pipes.back(), header);
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
