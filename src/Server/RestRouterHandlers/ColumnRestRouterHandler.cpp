#include "ColumnRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <Interpreters/Streaming/DDLHelper.h>

#include <boost/algorithm/string/join.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_STREAM;
    extern const int ILLEGAL_COLUMN;
    extern const int NO_SUCH_COLUMN_IN_STREAM;
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
                    {"alias", "string"},
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
                    {"alias", "string"},
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

std::pair<String, Int32> ColumnRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    const String & table = getPathParameter("table");
    const String & column = payload->get("name");

    if (Streaming::assertColumnExists(database, table, column, query_context))
        return {fmt::format("the stream '{}.{}' already has the column '{}'", database, table, column), HTTPResponse::HTTP_CONFLICT};

    setupDistributedQueryParameters({{"query_method", HTTPRequest::HTTP_POST}}, payload);

    std::vector<String> create_segments;
    create_segments.push_back("ALTER STREAM " + database + ".`" + table + "`");
    create_segments.push_back("ADD COLUMN ");
    create_segments.push_back(getCreateColumnDefinition(payload));
    const String & query = boost::algorithm::join(create_segments, " ");

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> ColumnRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload) const
{
    const String & table = getPathParameter("table");
    String column = getPathParameter("column");

    if (!Streaming::assertColumnExists(database, table, column, query_context))
        return {fmt::format("the stream '{}.{}' does not contain the column '{}'", database, table, column), HTTPResponse::HTTP_NOT_FOUND};

    setupDistributedQueryParameters({{"query_method", HTTPRequest::HTTP_PATCH}, {"column", column}}, payload);

    std::vector<String> update_segments;
    update_segments.push_back("ALTER STREAM " + database + ".`" + table + "`");
    update_segments.push_back(getUpdateColumnDefinition(query_context, payload, database, table, column));
    const String & query = boost::algorithm::join(update_segments, " ");

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> ColumnRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */) const
{
    const String & column = getPathParameter("column");
    const String & table = getPathParameter("table");

    if (!Streaming::assertColumnExists(database, table, column, query_context))
        return {fmt::format("the stream '{}.{}' does not contain the column '{}'", database, table, column), HTTPResponse::HTTP_NOT_FOUND};

    setupDistributedQueryParameters({{"query_method", HTTPRequest::HTTP_DELETE}, {"column", column}});

    std::vector<String> delete_segments;
    delete_segments.push_back("ALTER STREAM " + database + ".`" + table + "`");
    delete_segments.push_back("DROP COLUMN `" + column + "`");
    const String & query = boost::algorithm::join(delete_segments, " ");

    return {processQuery(query), HTTPResponse::HTTP_OK};
}
}
