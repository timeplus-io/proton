#include "ColumnRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <Core/Block.h>
#include <DistributedMetadata/CatalogService.h>

#include <boost/algorithm/string/join.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace
{
std::map<String, std::map<String, String> > CREATE_SCHEMA = {
    {"required",{
                    {"name","string"},
                    {"type", "string"},
                }
    },
    {"optional", {
                    {"nullable", "bool"},
                    {"default", "string"},
                    {"compression_codec", "string"},
                    {"ttl_expression", "string"},
                    {"skipping_index_expression", "string"}
                }
    }
};

std::map<String, std::map<String, String> > UPDATE_SCHEMA = {
    {"required",{
                }
    },
    {"optional", {
                    {"name", "string"},
                    {"comment", "string"},
                    {"type", "string"},
                    {"ttl_expression", "string"},
                    {"default", "string"},
                    {"skipping_index_expression", "string"}
                }
    }
};
}

bool ColumnRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(CREATE_SCHEMA, payload, error_msg);
}

bool ColumnRestRouterHandler::validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(UPDATE_SCHEMA, payload, error_msg);
}

String ColumnRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    const String & table = getPathParameter("table");
    const String & column = payload->get("name");

    auto [assert, message] = assertColumnNotExists(table, column);
    if (!assert)
    {
        http_status = HTTPResponse::HTTP_BAD_REQUEST;
        return message;
    }

    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({{"query_method", HTTPRequest::HTTP_POST}, {"column", column}}, payload);
    }

    std::vector<String> create_segments;
    create_segments.push_back("ALTER TABLE " + database + "." + table);
    create_segments.push_back("ADD COLUMN ");
    create_segments.push_back(getCreateColumnDefination(payload));
    const String & query = boost::algorithm::join(create_segments, " ");

    return processQuery(query, query_context);
}

String ColumnRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    const String & table = getPathParameter("table");
    String column = getPathParameter("column");

    auto [assert, message] = assertColumnExists(table, column);
    if (!assert)
    {
        http_status = HTTPResponse::HTTP_BAD_REQUEST;
        return message;
    }

    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({{"query_method", HTTPRequest::HTTP_PATCH}, {"column", column}}, payload);
    }

    std::vector<String> update_segments;
    update_segments.push_back("ALTER TABLE " + database + "." + table);
    update_segments.push_back(getUpdateColumnDefination(payload, column));
    const String & query = boost::algorithm::join(update_segments, " ");

    return processQuery(query, query_context);
}

String ColumnRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
{
    const String & column = getPathParameter("column");
    const String & table = getPathParameter("table");

    auto [assert, message] = assertColumnExists(table, column);
    if (!assert)
    {
        http_status = HTTPResponse::HTTP_BAD_REQUEST;
        return message;
    }

    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({{"query_method", HTTPRequest::HTTP_DELETE}, {"column", column}});
    }

    std::vector<String> delete_segments;
    delete_segments.push_back("ALTER TABLE " + database + "." + table);
    delete_segments.push_back("DROP COLUMN " + column);
    const String & query = boost::algorithm::join(delete_segments, " ");

    return processQuery(query, query_context);
}

std::pair<bool, String> ColumnRestRouterHandler::assertColumnExists(const String & table, const String & column) const
{
    const auto & catalog_service = CatalogService::instance(query_context);
    auto [table_exist, column_exist] = catalog_service.columnExists(database, table, column);

    if (!table_exist)
    {
        return {false, jsonErrorResponse(fmt::format("TABLE {} does not exist.", table), ErrorCodes::UNKNOWN_TABLE)};
    }

    if (!column_exist)
    {
        return {false, jsonErrorResponse(fmt::format("Column {} does not exist.", column), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE)};
    }

    return {true, ""};
}

std::pair<bool, String> ColumnRestRouterHandler::assertColumnNotExists(const String & table, const String & column) const
{
    const auto & catalog_service = CatalogService::instance(query_context);
    auto [table_exist, column_exist] = catalog_service.columnExists(database, table, column);

    if (!table_exist)
    {
        return {false, jsonErrorResponse(fmt::format("TABLE {} does not exist.", table), ErrorCodes::UNKNOWN_TABLE)};
    }

    if (column_exist)
    {
        return {false, jsonErrorResponse(fmt::format("Column {} already exists.", column), ErrorCodes::ILLEGAL_COLUMN)};
    }

    return {true, ""};
}

}
