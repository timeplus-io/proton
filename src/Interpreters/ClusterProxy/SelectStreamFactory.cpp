#include <DataTypes/ObjectUtils.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Storages/VirtualColumnUtils.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/checkStackSize.h>

#include <Client/IConnections.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event DistributedConnectionMissingTable;
extern const Event DistributedConnectionStaleReplica;
}

namespace DB
{

namespace ErrorCodes
{
extern const int ALL_REPLICAS_ARE_STALE;
}

namespace ClusterProxy
{

/// select query has database, table and table function names as AST pointers
/// Creates a copy of query, changes database, table and table function names.
ASTPtr rewriteSelectQuery(
    ContextPtr context,
    const ASTPtr & query,
    const std::string & remote_database,
    const std::string & remote_table,
    ASTPtr table_function_ptr)
{
    auto modified_query_ast = query->clone();

    ASTSelectQuery & select_query = modified_query_ast->as<ASTSelectQuery &>();

    // Get rid of the settings clause so we don't send them to remote. Thus newly non-important
    // settings won't break any remote parser. It's also more reasonable since the query settings
    // are written into the query context and will be sent by the query pipeline.
    select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, {});

    if (table_function_ptr)
        select_query.addTableFunction(table_function_ptr);
    else
        select_query.replaceDatabaseAndTable(remote_database, remote_table);

    /// Restore long column names (cause our short names are ambiguous).
    /// TODO: aliased table functions & CREATE TABLE AS table function cases
    if (!table_function_ptr)
    {
        RestoreQualifiedNamesVisitor::Data data;
        data.distributed_table = DatabaseAndTableWithAlias(*getTableExpression(query->as<ASTSelectQuery &>(), 0));
        data.remote_table.database = remote_database;
        data.remote_table.table = remote_table;
        RestoreQualifiedNamesVisitor(data).visit(modified_query_ast);
    }

    /// To make local JOIN works, default database should be added to table names.
    /// But only for JOIN section, since the following should work using default_database:
    /// - SELECT * FROM d WHERE value IN (SELECT l.value FROM l) ORDER BY value
    ///   (see 01487_distributed_in_not_default_db)
    AddDefaultDatabaseVisitor visitor(
        context,
        context->getCurrentDatabase(),
        /* only_replace_current_database_function_= */ false,
        /* only_replace_in_join_= */ true);
    visitor.visit(modified_query_ast);

    return modified_query_ast;
}

SelectStreamFactory::SelectStreamFactory(
    const Block & header_,
    const ColumnsDescriptionByShardNum & objects_by_shard_,
    const StorageSnapshotPtr & storage_snapshot_,
    QueryProcessingStage::Enum processed_stage_)
    : header(header_), objects_by_shard(objects_by_shard_), storage_snapshot(storage_snapshot_), processed_stage(processed_stage_)
{
}

void SelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const ASTPtr & query_ast,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    ContextPtr context,
    std::vector<QueryPlanPtr> & local_plans,
    Shards & remote_shards,
    UInt32 shard_count,
    bool parallel_replicas_enabled,
    AdditionalShardFilterGenerator shard_filter_generator)
{

    auto emplace_local_stream = [&]() {
        /// HACKY : Setup query_kind to break infinite query forwarding loop
        auto mutable_context = std::const_pointer_cast<Context>(context);
        auto & client_info = mutable_context->getClientInfo();
        client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        local_plans.emplace_back(createLocalPlan(
            query_ast,
            header,
            context,
            processed_stage,
            shard_info.shard_num,
            shard_count,
            /*has_missing_objects=*/false));
    };

    /// If lazy is true, a lazy pipe will be created. It will try to use the local replica and,
    /// if not possible, will use DelayedSource for reading from remote replica.
    auto emplace_remote_stream = [&](bool lazy = false) {
        remote_shards.emplace_back(Shard{
            .query = query_ast,
            .header = header,
            .shard_info = shard_info,
            .lazy = lazy,
            .shard_filter_generator = std::move(shard_filter_generator),
        });
    };

    const auto & settings = context->getSettingsRef();

    /// prefer_localhost_replica is not effective in case of parallel replicas
    /// (1) prefer_localhost_replica is about choosing one replica on a shard
    /// (2) parallel replica coordinator has own logic to choose replicas to read from
    if (settings.prefer_localhost_replica && shard_info.isLocal() && !parallel_replicas_enabled)
    {
        StoragePtr main_table_storage;

        if (table_func_ptr)
        {
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_func_ptr, context);
            main_table_storage = table_function_ptr->execute(table_func_ptr, context, table_function_ptr->getName());
        }
        else
        {
            auto resolved_id = context->resolveStorageID(main_table);
            main_table_storage = DatabaseCatalog::instance().tryGetTable(resolved_id, context);
        }

        if (!main_table_storage) /// Table is absent on a local server.
        {
            ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);
            if (shard_info.hasRemoteConnections())
            {
                LOG_WARNING(getLogger("ClusterProxy::SelectStreamFactory"),
                    "There is no stream {} on local replica of shard {}, will try remote replicas.",
                    main_table.getNameForLogs(),
                    shard_info.shard_num);

                emplace_remote_stream();
            }
            else
            {
                emplace_local_stream(); /// Let it fail the usual way.
            }

            return;
        }

        emplace_local_stream();
    }
    else
    {
        emplace_remote_stream();
    }
}
}
}
