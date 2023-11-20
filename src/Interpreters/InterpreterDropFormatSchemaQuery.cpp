#include <Access/ContextAccess.h>
#include <Formats/FormatSchemaFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropFormatSchemaQuery.h>
#include <Parsers/ASTDropFormatSchemaQuery.h>

namespace DB
{

BlockIO InterpreterDropFormatSchemaQuery::execute()
{
    ASTDropFormatSchemaQuery & drop_format_schema_query = query_ptr->as<ASTDropFormatSchemaQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_FORMAT_SCHEMA);

    auto current_context = getContext();

    if (!drop_format_schema_query.cluster.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ON CLUSTER is not implemented for DROP FORMAT SCHEMA");

    current_context->checkAccess(access_rights_elements);

    FormatSchemaFactory::instance().unregisterSchema(current_context, drop_format_schema_query.schema_name, drop_format_schema_query.schema_type, !drop_format_schema_query.if_exists);

    return {};
}

}
