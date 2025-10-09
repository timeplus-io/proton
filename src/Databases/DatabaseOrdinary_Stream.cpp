#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DatabaseOrdinary.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListAlertsRequest.h>
#include <Cluster/Requests/ListAlertsResponse.h>
#include <Cluster/Requests/ListStreamsRequest.h>
#include <Cluster/Requests/ListStreamsResponse.h>
#include <Interpreters/Context.h>
#include <Interpreters/MetadataHelper.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/Alert/StorageAlert.h>


namespace DB
{

namespace
{
TableNamesSet getAlertDependency(const cluster::protocol::AlertDescriptorPtr & desc, ContextPtr context)
{
    ParserSelectQuery select_p;
    String parse_err;
    const auto * query_text = desc->select_query.data();
    auto select_ast = tryParseQuery(
        select_p,
        query_text,
        query_text + desc->select_query.size(),
        parse_err,
        /*hilite=*/false,
        "get alert dependencies",
        /*allow_multi_statements=*/false,
        /*max_query_size=*/0,
        context->getSettingsRef().max_parser_depth);

    chassert(select_ast);

    const auto & select = select_ast->as<ASTSelectQuery &>();
    chassert(select.tables());

    const auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    chassert(tables_in_select_query.children.size() == 1);

    const auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    chassert(tables_element.table_expression);

    const auto & table_expression = tables_element.table_expression->as<ASTTableExpression &>();
    chassert(table_expression.database_and_table_name != nullptr);

    auto & id = table_expression.database_and_table_name->as<ASTTableIdentifier &>();
    auto table_id = id.getTableId();

    return {{.database = table_id.database_name, .table = table_id.table_name}};
}
}

void DatabaseOrdinary::loadTablesMetadataFromMetaStore(const ContextPtr & local_context, ParsedTablesMetadata & metadata)
{
    auto timeout_ms = local_context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();

    /// proton: starts - Single-instance: use metastore node ID
    auto request = std::make_shared<cluster::ListStreamsRequest>(
        database_name, /*stream=*/"", metastore.nodeID(), /*consistent_read_=*/false, timeout_ms, /*request_version=*/2);
    /// proton: ends
    auto resp = metastore.listStreams(std::move(request));
    if (resp->hasError())
    {
        LOG_ERROR(log, "Failed to get streams for database={} from MetaStore, reason={}", database_name, resp->data().err.string());

        throw Exception(
            resp->data().err.error_code,
            "Failed to get streams for database={} from MetaStore, reason={}",
            database_name,
            resp->data().err.error_message);
    }

    size_t prev_tables_count = metadata.parsed_tables.size();
    size_t prev_total_dictionaries = metadata.total_dictionaries;

    auto settings = local_context->getSettingsRef();

    for (const auto & stream : resp->data().stream_descs)
    {
        auto ast = getCreateTableFromStreamDescription(*stream, local_context);
        const auto & create = ast->as<const ASTCreateQuery &>();

        /// FIXME, handle detached

        QualifiedTableName qualified_name{.database = database_name, .table = create.getTable()};
        TableNamesSet loading_dependencies = getDependenciesSetFromCreateQuery(getContext(), qualified_name, ast);

        {
            std::lock_guard lock{metadata.mutex};
            metadata.parsed_tables[qualified_name] = ParsedTableMetadata{
                .path = "",
                .ast = ast,
                .schema_version = stream->data_version,
            };

            if (loading_dependencies.empty())
            {
                metadata.independent_database_objects.emplace_back(std::move(qualified_name));
            }
            else
            {
                for (const auto & dependency : loading_dependencies)
                    metadata.dependencies_info[dependency].dependent_database_objects.insert(qualified_name);
                assert(metadata.dependencies_info[qualified_name].dependencies.empty());
                metadata.dependencies_info[qualified_name].dependencies = std::move(loading_dependencies);
            }
            metadata.total_dictionaries += create.isDictionary();
        }
    }

    size_t objects_in_database = metadata.parsed_tables.size() - prev_tables_count;
    size_t dictionaries_in_database = metadata.total_dictionaries - prev_total_dictionaries;
    size_t tables_in_database = objects_in_database - dictionaries_in_database;

    LOG_INFO(
        log,
        "MetaStore processed, database {} has {} distributed tables and {} dictionaries in total.",
        database_name,
        tables_in_database,
        dictionaries_in_database);
}

void DatabaseOrdinary::loadAlertsMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata)
{
    /// timeout_ms unused in single-instance mode
    [[maybe_unused]] auto timeout_ms = local_context->getSettingsRef().query_timeout_sec * 1000;

    auto & metastore = Globals::getMetaStore();

    auto list_alerts_req = std::make_shared<cluster::ListAlertsRequest>(
        database_name, /// namespace
        "", /// name (empty for listing all)
        0, /// initiator (single-instance: always 0)
        false, /// consistent_read
        timeout_ms, /// timeout_ms
        1 /// request_version
    );
    auto list_alerts_resp = metastore.listAlerts(list_alerts_req);

    if (list_alerts_resp->hasError())
    {
        LOG_ERROR(
            log, "Failed to get alerts for database={} from MetaStore, reason={}", database_name, list_alerts_resp->data().err.string());

        throw Exception(
            list_alerts_resp->data().err.error_code,
            "Failed to get alerts for database={} from MetaStore, reason={}",
            database_name,
            list_alerts_resp->data().err.error_message);
    }

    alert_descriptors.reserve(list_alerts_resp->data().descs.size());

    for (const auto & desc : list_alerts_resp->data().descs)
    {
        QualifiedTableName qualified_name{.database = desc->ns, .table = desc->name};
        auto dependencies = getAlertDependency(desc, local_context);

        {
            std::lock_guard lock{metadata.mutex};
            metadata.parsed_tables[qualified_name] = {};
            chassert(!dependencies.empty());
            for (const auto & dependency : dependencies)
                metadata.dependencies_info[dependency].dependent_database_objects.insert(qualified_name);
            assert(metadata.dependencies_info[qualified_name].dependencies.empty());
            metadata.dependencies_info[qualified_name].dependencies = std::move(dependencies);
        }

        alert_descriptors.insert({desc->name, desc});
    }

    LOG_INFO(log, "Alerts metadata loaded, database {} has {} alerts", database_name, alert_descriptors.size());
}

void DatabaseOrdinary::loadAlert(const QualifiedTableName & name, ContextMutablePtr local_context)
{
    auto desc = alert_descriptors.at(name.table);
    auto alert = std::make_shared<DB::StorageAlert>(
        DB::StorageID{desc->ns, desc->name, desc->id}, desc, StorageAlert::ValidationLevel::None, local_context);
    attachTable(local_context, desc->name, alert, "alert");
}

}
