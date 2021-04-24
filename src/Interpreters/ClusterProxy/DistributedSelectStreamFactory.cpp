#include "DistributedSelectStreamFactory.h"

#include <DataStreams/RemoteQueryExecutor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/RemoteSource.h>


namespace DB
{
namespace ClusterProxy
{
namespace
{
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
    std::vector<QueryPlanPtr> & /* plans */,
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

    String query = formattedAST(query_ast);

    auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
        shard_info.pool, query, header, context, throttler, scalars, external_tables, processed_stage);

    remote_query_executor->setLogger(log);
    remote_query_executor->setPoolMode(PoolMode::GET_MANY);
    remote_query_executor->setMainTable(main_table);

    remote_pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read));
    remote_pipes.back().addInterpreterContext(context);
}
}
}
