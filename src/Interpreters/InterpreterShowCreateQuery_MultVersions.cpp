#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Interpreters/StorageID.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Core/Block.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED;
}

namespace
{

String canonicalizeCreateStatement(const String & sql_def, const StorageID & table_id, ContextPtr query_context)
{
    ASTPtr ast;
    try
    {
        ast = DB::parseQuery(sql_def, query_context);
    }
    catch (...)
    {
        return sql_def;
    }

    auto * create = ast->as<ASTCreateQuery>();
    if (!create)
        return sql_def;

    create->attach = false;
    create->uuid = UUIDHelpers::Nil;
    create->to_inner_uuid = UUIDHelpers::Nil;
    create->setDatabase(table_id.database_name);
    create->setTable(table_id.table_name);

    if (create->is_input && create->storage)
        create->storage->engine = nullptr;

    return ast->formatWithPossiblyHidingSensitiveData(/*max_length=*/0, /*one_line=*/false, /*show_secrets=*/false);
}

}

QueryPipeline InterpreterShowCreateQuery::showMultiVersions(const StorageID & table_id) const
{
    auto & metastore = Globals::getMetaStore();
    const auto & meta_store = Globals::getMetaStore();

    Block result = getSampleBlock();

    /// column positions
    /// 0 - statement
    /// 1 - version
    /// 2 - last_modified
    /// 3 - last_modified_by
    /// 4 - created
    /// 5 - created_by
    /// 6 - uuid
    if (query_ptr->as<ASTShowCreateTableQuery>() || query_ptr->as<ASTShowCreateViewQuery>()
        || query_ptr->as<ASTShowCreateDictionaryQuery>())
    {
        auto req = std::make_shared<cluster::GetStreamRequest>(
            table_id.database_name,
            table_id.table_name,
            meta_store.getConfig().metadata_keep_versions,
            /*initiator_=*/meta_store.nodeID(),
            /*consistent_read_=*/false,
            /*timeout_ms_=*/5'000,
            /*request_version=*/1);
        auto response = metastore.getStream(std::move(req));
        if (response->hasError())
            throw Exception(
                response->error().error_code,
                "Failed to read all versions of stream or view or dictionary {}, {}",
                table_id.getFullTableName(),
                response->error().string());

        const auto & req_data = response->data();
        result.reserve(req_data.stream_descs.size());
        auto columns = result.mutateColumns();

        for (auto riter = req_data.stream_descs.rbegin(); riter != req_data.stream_descs.rend(); ++riter)
        {
            const auto & desc = *riter;
            const auto statement = canonicalizeCreateStatement(desc->sql_def, table_id, getContext());
            columns[0]->insertData(statement.data(), statement.size());
            columns[1]->insert(desc->data_version);
            columns[2]->insert(DateTime64(desc->last_modify_timestamp_ms));
            columns[3]->insertData(desc->last_modified_by.c_str(), desc->last_modified_by.size());
            columns[4]->insert(DateTime64(desc->create_timestamp_ms));
            columns[5]->insertData(desc->created_by.c_str(), desc->created_by.size());
            columns[6]->insert(desc->stream.id);
        }
        result.setColumns(std::move(columns));
    }
    else if (auto * show_query = query_ptr->as<ASTShowCreateDatabaseQuery>())
    {
        auto req = std::make_shared<cluster::GetDatabaseRequest>(
            table_id.database_name,
            meta_store.getConfig().metadata_keep_versions,
            /*initiator_=*/meta_store.nodeID(),
            /*consistent_read_=*/false,
            /*timeout_ms_=*/5'000,
            /*request_version=*/1);

        /// proton: starts
        /// Single-instance: no use_local parameter
        auto response = metastore.getDatabase(std::move(req));
        /// proton: ends
        if (response->hasError())
            throw Exception(
                response->error().error_code,
                "Failed to read all versions of stream database {}, {}",
                show_query->getDatabase(),
                response->error().string());

        const auto & req_data = response->data();
        result.reserve(req_data.databases.size());
        auto columns = result.mutateColumns();

        for (auto riter = req_data.databases.rbegin(); riter != req_data.databases.rend(); ++riter)
        {
            const auto & desc = *riter;
            columns[0]->insertData(desc->sql_def.c_str(), desc->sql_def.size());
            columns[1]->insert(1); /// FIXME, when we have data_version for database as well
            columns[2]->insert(desc->last_modify_timestamp_ms);
            columns[3]->insertData(desc->last_modified_by.c_str(), desc->last_modified_by.size());
            columns[4]->insert(desc->create_timestamp_ms);
            columns[5]->insertData(desc->created_by.c_str(), desc->created_by.size());
            columns[6]->insert(table_id.uuid);
        }
    }
    else
    {
        throw Exception(
            ErrorCodes::UNSUPPORTED, "Showing multi-version of the create query of {} is not supported", table_id.getFullTableName());
    }

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(result)));
}

}
