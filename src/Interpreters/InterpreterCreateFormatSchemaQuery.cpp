#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateFormatSchemaQuery.h>
#include <Parsers/ASTCreateFormatSchemaQuery.h>

/// proton: starts
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/CreateFormatSchemaRequest.h>
#include <Cluster/Requests/ListFormatSchemasRequest.h>
/// proton: ends

#include <algorithm>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_FORMAT_SCHEMA;
extern const int FORMAT_SCHEMA_ALREADY_EXISTS;
}

namespace
{
/// \return a view of the input string with both leading and trailing whitespaces are removed.
std::string_view trim(const String & str)
{
    std::string_view ret;
    auto begin = std::ranges::find_if_not(str, [](auto ch) { return std::isspace(ch); });
    if (begin == str.end())
        return ret;

    auto end = std::ranges::find_if_not(std::ranges::reverse_view(str), [](auto ch) { return std::isspace(ch); });
    size_t size = end.base() - begin;
    return std::string_view{begin.base(), size};
}

}

BlockIO InterpreterCreateFormatSchemaQuery::execute()
{
    auto & create_format_schema_query = query_ptr->as<ASTCreateFormatSchemaQuery &>();

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

    auto exists_op = cluster::protocol::ExistsOperation::Throw;
    if (create_format_schema_query.if_not_exists)
        exists_op = cluster::protocol::ExistsOperation::Ignore;
    else if (create_format_schema_query.or_replace)
        exists_op = cluster::protocol::ExistsOperation::Replace;

    auto timeout_ms = current_context->getSettingsRef().query_timeout_sec * 1000;

    auto & metastore = Globals::getMetaStore();
    UInt32 version{1};
    auto list_req = std::make_shared<cluster::ListFormatSchemasRequest>(
        create_format_schema_query.getSchemaName(),
        create_format_schema_query.schema_type,
        0, // initiator
        false, // consistent_read
        30000, // timeout_ms
        1 // request_version
    );
    auto list_resp = metastore.listFormatSchemas(list_req);

    if (const auto err = list_resp->error(); err.hasError())
    {
        if (!err.isNotFound())
            throw Exception(
                ErrorCodes::CANNOT_CREATE_FORMAT_SCHEMA,
                "Failed to create check the existence of format schema {}: error_code={} error_message={}",
                create_format_schema_query.getSchemaName(),
                err.error_code,
                err.error_message);
    }
    else
    {
        const auto schema_descriptions = list_resp->data().descs;
        if (schema_descriptions.size() > 1)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Found more than one existing format schemas of type {} with name {}",
                create_format_schema_query.schema_type,
                create_format_schema_query.getSchemaName());

        if (schema_descriptions.size() == 1)
        {
            if (exists_op == cluster::protocol::ExistsOperation::Ignore)
                return {};
            else if (exists_op == cluster::protocol::ExistsOperation::Throw)
                throw Exception(
                    ErrorCodes::FORMAT_SCHEMA_ALREADY_EXISTS,
                    "{} format schema {} already exists",
                    create_format_schema_query.schema_type,
                    create_format_schema_query.getSchemaName());
            else
                version = schema_descriptions[0]->data_version;
        }
    }

    /// proton: starts - Single-instance: use metastore node ID
    auto create_format_schema_req = std::make_shared<cluster::CreateFormatSchemaRequest>(
        create_format_schema_query.getSchemaName(),
        create_format_schema_query.schema_type,
        std::string(trimmed_body),
        exists_op,
        current_context->getUserName(),
        version,
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/2);
    /// proton: ends
    auto create_format_schema_resp = metastore.createFormatSchema(std::move(create_format_schema_req));
    if (create_format_schema_resp->hasError())
    {
        const auto & err = create_format_schema_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_CREATE_FORMAT_SCHEMA,
            "Failed to create format schema {}: error_code={} error_message={}",
            create_format_schema_query.getSchemaName(),
            err.error_code,
            err.error_message);
    }

    return {};
}
}
