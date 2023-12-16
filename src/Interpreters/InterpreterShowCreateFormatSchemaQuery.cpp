#include <Access/Common/AccessRightsElement.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatSchemaFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateFormatSchemaQuery.h>
#include <Parsers/ASTCreateFormatSchemaQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowCreateFormatSchemaQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Parsers/formatAST.h>

namespace DB
{

BlockIO InterpreterShowCreateFormatSchemaQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


Block InterpreterShowCreateFormatSchemaQuery::getSampleBlock()
{
    return Block{{
        ColumnString::create(),
        std::make_shared<DataTypeString>(),
        "statement"}};
}


QueryPipeline InterpreterShowCreateFormatSchemaQuery::executeImpl()
{
    auto * show_query = query_ptr->as<ASTShowCreateFormatSchemaQuery>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_FORMAT_SCHEMAS);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    const auto & schema = FormatSchemaFactory::instance().getSchema(show_query->getSchemaName(), show_query->schema_type, current_context);

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

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}}));
}

}
