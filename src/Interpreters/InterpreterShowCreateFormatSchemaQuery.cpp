#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateFormatSchemaQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSchemaFactory.h>
#include <Parsers/ASTCreateFormatSchemaQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowCreateFormatSchemaQuery.h>
#include <Parsers/formatAST.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

namespace DB
{

BlockIO InterpreterShowCreateFormatSchemaQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}

Block InterpreterShowCreateFormatSchemaQuery::getSampleBlock() const
{
    if (getContext()->getSettingsRef().show_multi_versions)
        return Block{
            {ColumnString::create(), std::make_shared<DataTypeString>(), "statement"},
            {ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), "version"},
            {ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "last_modified"},
            {ColumnString::create(), std::make_shared<DataTypeString>(), "last_modified_by"},
            {ColumnDateTime64::create(0, 3), std::make_shared<DataTypeDateTime64>(3, "UTC"), "created"},
            {ColumnString::create(), std::make_shared<DataTypeString>(), "created_by"},
        };
    else
        return Block{{ColumnString::create(), std::make_shared<DataTypeString>(), "statement"}};
}

QueryPipeline InterpreterShowCreateFormatSchemaQuery::executeImpl()
{
    const auto * show_query = query_ptr->as<ASTShowCreateFormatSchemaQuery>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_FORMAT_SCHEMAS);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    const auto & schema = FormatSchemaFactory::instance().getSchema(show_query->getSchemaName(), show_query->schema_type, current_context);

    if (current_context->getSettingsRef().show_multi_versions)
        return showMultiVersions(schema.name, schema.type);

    ASTCreateFormatSchemaQuery create_query;
    create_query.schema_type = schema.type;
    create_query.schema_name = show_query->schema_name;
    create_query.children.push_back(create_query.schema_name);
    create_query.schema_core = std::make_shared<ASTLiteral>(schema.body);
    create_query.children.push_back(create_query.schema_core);

    WriteBufferFromOwnString buf;
    formatAST(create_query, buf, false, false);
    String res = buf.str();

    MutableColumnPtr column = ColumnString::create();
    column->insert(res);

    return QueryPipeline(
        std::make_shared<SourceFromSingleChunk>(Block{{std::move(column), std::make_shared<DataTypeString>(), "statement"}}));
}

QueryPipeline InterpreterShowCreateFormatSchemaQuery::showMultiVersions(const String & schema_name, const String & schema_type) const
{
    const auto & meta_store = Globals::getMetaStore();
    auto & metastore = Globals::getMetaStore();

    auto req = std::make_shared<cluster::GetFormatSchemaRequest>(
        schema_name,
        schema_type,
        meta_store.getConfig().metadata_keep_versions,
        /*initiator_=*/meta_store.nodeID(),
        /*consistent_read_=*/false,
        /*timeout_ms_=*/5'000,
        /*request_version=*/1);

    /// proton: starts
    /// Single-instance mode: remove use_local parameter
    auto response = metastore.getFormatSchema(std::move(req));
    /// proton: ends
    if (response->hasError())
        throw Exception(
            response->error().error_code,
            "Failed to read all versions of format schema schema_name={} schema_type={} {}",
            schema_name,
            schema_type,
            response->error().string());

    const auto & resp_data = response->data();
    Block result = getSampleBlock();
    result.reserve(resp_data.descs.size());
    auto columns = result.mutateColumns();

    auto schema_name_ast = std::make_shared<ASTLiteral>(schema_name);
    ASTCreateFormatSchemaQuery create_query;
    create_query.schema_type = schema_type;
    create_query.schema_name = schema_name_ast;
    create_query.children.reserve(2);
    create_query.children.push_back(create_query.schema_name);
    create_query.children.push_back(nullptr);

    for (auto riter = resp_data.descs.rbegin(); riter != resp_data.descs.rend(); ++riter)
    {
        const auto & desc = *riter;

        create_query.schema_core = std::make_shared<ASTLiteral>(desc->schema_body);
        create_query.children.back() = create_query.schema_core;

        WriteBufferFromOwnString buf;
        formatAST(create_query, buf, false, false);
        String statement = buf.str();

        /// 0 - statement
        /// 1 - version
        /// 2 - last_modified
        /// 3 - last_modified_by
        /// 4 - created
        /// 5 - created_by
        columns[0]->insertData(statement.c_str(), statement.size());
        columns[1]->insert(desc->data_version);
        columns[2]->insert(DateTime64(desc->last_modify_timestamp_ms));
        columns[3]->insertData(desc->last_modified_by.data(), desc->last_modified_by.size());
        columns[4]->insert(DateTime64(desc->create_timestamp_ms));
        columns[5]->insertData(desc->created_by.data(), desc->created_by.size());
    }

    result.setColumns(std::move(columns));

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(result)));
}

}
