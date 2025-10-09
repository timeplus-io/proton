#include <Interpreters/InterpreterShowAlertsQuery.h>
#include <Interpreters/Context.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListAlertsRequest.h>
#include <Cluster/Requests/ListAlertsResponse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/ASTShowAlertsQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_DATABASE;
}

namespace
{

Block getSampleBlock(bool verbose)
{
    Block block;
    block.reserve(verbose ? 7 : 2);
    block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "name"});
    block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "uuid"});
    if (verbose)
    {
        block.insert({ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "version"});
        block.insert({ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "last_modified"});
        block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "last_modified_by"});
        block.insert({ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "created"});
        block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "created_by"});
    }

    return block;
}

void insertRecord(Block & block, const cluster::protocol::AlertDescriptor & desc, bool verbose)
{
    auto columns = block.mutateColumns();

    columns[0]->insertData(desc.name.data(), desc.name.size());
    auto uuid = toString(desc.id);
    columns[1]->insertData(uuid.data(), uuid.size());
    if (verbose)
    {
        columns[2]->insert(desc.data_version);
        columns[3]->insert(DateTime64(desc.last_modify_timestamp_ms));
        columns[4]->insertData(desc.last_modified_by.data(), desc.last_modified_by.size());
        columns[5]->insert(DateTime64(desc.create_timestamp_ms));
        columns[6]->insertData(desc.created_by.data(), desc.created_by.size());
    }

    block.setColumns(std::move(columns));
}

cluster::protocol::AlertDescriptorPtrs getAlerts(const String & database)
{
    auto & metastore = Globals::getMetaStore();
    auto req = std::make_shared<cluster::ListAlertsRequest>(
        database,
        "",     // name (empty for all alerts)
        0,      // initiator (single-instance: always 0)
        false,  // consistent_read
        30000,  // timeout_ms
        1       // request_version
    );
    auto resp = metastore.listAlerts(req);
    if (resp->hasError())
        throw Exception(resp->error().error_code, "Failed to fetch alerts: {}", resp->error().error_message);

    return std::move(resp->data().descs);
}

}

BlockIO InterpreterShowAlertsQuery::execute()
{
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_ALERTS);
    getContext()->checkAccess(access_rights_elements);

    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}

QueryPipeline InterpreterShowAlertsQuery::executeImpl()
{
    auto & query = query_ptr->as<ASTShowAlertsQuery &>();
    auto current_context = getContext();

    std::string database;
    if (query.database)
    {
        database = *query.database;
        if (!DatabaseCatalog::instance().isDatabaseExist(database))
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", database);
    }
    else
    {
        database = current_context->getCurrentDatabase();
    }

    auto alerts = getAlerts(database);

    bool verbose{current_context->getSettingsRef().verbose.value};
    auto block = getSampleBlock(verbose);
    for (const auto & alert : alerts)
        insertRecord(block, *alert, verbose);

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(block)));
}

}
