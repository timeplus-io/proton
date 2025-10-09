#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowTasksQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListTasksRequest.h>
#include <Cluster/Requests/ListTasksResponse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Parsers/ASTShowTasksQuery.h>
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
    block.reserve(verbose ? 8 : 2);
    block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "name"});
    block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "uuid"});
    if (verbose)
    {
        block.insert({ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "version"});
        block.insert({ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "status"});
        block.insert({ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "last_modified"});
        block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "last_modified_by"});
        block.insert({ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "created"});
        block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "created_by"});
    }

    return block;
}

void insertRecord(Block & block, const cluster::protocol::TaskDescriptor & desc, bool verbose)
{
    auto columns = block.mutateColumns();

    size_t col_idx = 0;
    columns[col_idx++]->insertData(desc.name.data(), desc.name.size());
    auto uuid = toString(desc.id);
    columns[col_idx++]->insertData(uuid.data(), uuid.size());
    if (verbose)
    {
        columns[col_idx++]->insert(desc.data_version);
        columns[col_idx++]->insert(desc.status);
        columns[col_idx++]->insert(DateTime64(desc.last_modify_timestamp_ms));
        columns[col_idx++]->insertData(desc.last_modified_by.data(), desc.last_modified_by.size());
        columns[col_idx++]->insert(DateTime64(desc.create_timestamp_ms));
        columns[col_idx++]->insertData(desc.created_by.data(), desc.created_by.size());
    }

    block.setColumns(std::move(columns));
}

cluster::protocol::TaskDescriptorPtrs getTasks(const String & database)
{
    auto & metastore = Globals::getMetaStore();
    auto req = std::make_shared<cluster::ListTasksRequest>(
        database,
        "", // name (empty for all tasks)
        0, // initiator
        false, // consistent_read
        30000, // timeout_ms
        1 // request_version
    );
    auto resp = metastore.listTasks(req);
    if (resp->hasError())
        throw Exception(resp->error().error_code, "Failed to fetch tasks: {}", resp->error().error_message);

    return std::move(resp->data().descs);
}

}

BlockIO InterpreterShowTasksQuery::execute()
{
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_TASKS);
    getContext()->checkAccess(access_rights_elements);

    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}

QueryPipeline InterpreterShowTasksQuery::executeImpl()
{
    auto & query = query_ptr->as<ASTShowTasksQuery &>();
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

    auto tasks = getTasks(database);

    bool verbose{current_context->getSettingsRef().verbose.value};
    auto block = getSampleBlock(verbose);
    for (const auto & task : tasks)
        insertRecord(block, *task, verbose);

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(block)));
}

}
