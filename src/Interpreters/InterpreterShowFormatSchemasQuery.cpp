#include <Interpreters/InterpreterShowFormatSchemasQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListFormatSchemasRequest.h>
#include <Cluster/Requests/ListFormatSchemasResponse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatSchemaFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTShowFormatSchemasQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{

namespace
{

Block getSampleBlock(bool verbose)
{
    Block block;
    block.reserve(verbose ? 7 : 2);
    block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "name"});
    block.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "type"});
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

void insertRecord(Block & block, const cluster::protocol::FormatSchemaDescriptor & desc, bool verbose)
{
    auto columns = block.mutateColumns();

    columns[0]->insertData(desc.name.data(), desc.name.size());
    columns[1]->insertData(desc.format.data(), desc.format.size());
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

cluster::protocol::FormatSchemaDescriptorPtrs getSchemas()
{
    auto & metastore = Globals::getMetaStore();
    auto req = std::make_shared<cluster::ListFormatSchemasRequest>(
        "" /* schema_name */,
        "" /* format */,
        0 /* initiator */,
        /*consistent_read_=*/false,
        /*timeout_ms_=*/10'000,
        /*request_version_=*/2);
    auto resp = metastore.listFormatSchemas(req);
    if (resp->hasError())
        throw Exception(resp->error().error_code, "Failed to fetch format schemas: {}", resp->error().error_message);

    return std::move(resp->data().descs);
}

}

BlockIO InterpreterShowFormatSchemasQuery::execute()
{
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_FORMAT_SCHEMAS);
    getContext()->checkAccess(access_rights_elements);

    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}

QueryPipeline InterpreterShowFormatSchemasQuery::executeImpl()
{
    auto schemas = getSchemas();
    auto & query = query_ptr->as<ASTShowFormatSchemasQuery &>();

    bool verbose{getContext()->getSettingsRef().verbose.value};
    auto block = getSampleBlock(verbose);
    for (const auto & schema : schemas)
    {
        if (query.schema_type.empty() || schema->format == query.schema_type)
            insertRecord(block, *schema, verbose);
    }

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(block)));
}

}
