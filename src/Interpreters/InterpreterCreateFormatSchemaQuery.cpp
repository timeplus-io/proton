#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Formats/FormatSchemaFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateFormatSchemaQuery.h>
#include <Parsers/ASTCreateFormatSchemaQuery.h>

namespace DB
{

namespace
{
/// Retruns a view of the input string with both leading and trailing whitespaces are removed.
std::string_view trim(const String & str)
{
    std::string_view ret;
    auto begin = std::find_if_not(str.begin(), str.end(), [](auto ch) { return std::isspace(ch); });
    if (begin == str.end())
        return ret;
    auto end = std::find_if_not(str.rbegin(), str.rend(), [](auto ch) { return std::isspace(ch); });
    size_t size = end.base() - begin;
    return std::string_view{begin.base(), size};
}

}

BlockIO InterpreterCreateFormatSchemaQuery::execute()
{
    auto & create_format_schema_query = query_ptr->as<ASTCreateFormatSchemaQuery &>();

    if (!create_format_schema_query.cluster.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ON CLUSTER is not implemented for CREATE FORMAT SCHEMA");

    auto body = create_format_schema_query.getSchemaBody();
    auto trimmed_body = trim(body);
    if (trimmed_body.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Format schema body cannot be empty");

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FORMAT_SCHEMA);

    auto replace_if_exists = create_format_schema_query.or_replace;
    if (replace_if_exists)
        access_rights_elements.emplace_back(AccessType::DROP_FORMAT_SCHEMA);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    auto exists_op = FormatSchemaFactory::ExistsOP::Throw;
    if (create_format_schema_query.if_not_exists)
        exists_op = FormatSchemaFactory::ExistsOP::Ignore;
    else if (create_format_schema_query.or_replace)
        exists_op = FormatSchemaFactory::ExistsOP::Replace;

    FormatSchemaFactory::instance().registerSchema(create_format_schema_query.getSchemaName(), create_format_schema_query.schema_type, trimmed_body, exists_op, current_context);
    return {};
}
}
