#include "SystemCommandHandler.h"
#include "SchemaValidator.h"

#include <DistributedMetadata/CatalogService.h>

#include <Parsers/ASTSystemQuery.h>
#include <Common/ProtonCommon.h>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int STREAM_ALREADY_EXISTS;
extern const int UNKNOWN_STREAM;
extern const int NO_REPLICA_NAME_GIVEN;
}

std::map<String, std::map<String, String>> SystemCommandHandler::command_schema
    = {{"required", {{"type", "string"}, {"name", "string"}}},
       {"optional", {{"database", "string"}, {"stream", "string"}, {"shard", "int"}, {"old_replica", "string"}}}};

bool SystemCommandHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(command_schema, payload, error_msg))
        return false;

    auto type = ASTSystemQuery::stringToType(payload->get("type").toString());
    const auto & type_name = payload->get("type").toString();

    if (type != ASTSystemQuery::Type::DROP_REPLICA && type != ASTSystemQuery::Type::ADD_REPLICA
        && type != ASTSystemQuery::Type::STOP_MAINTAIN && type != ASTSystemQuery::Type::START_MAINTAIN
        && type != ASTSystemQuery::Type::REPLACE_REPLICA && type != ASTSystemQuery::Type::RESTART_REPLICA)
    {
        error_msg = fmt::format("Invalid command type {}", type_name);
        return false;
    }

    if (type == ASTSystemQuery::Type::START_MAINTAIN || type == ASTSystemQuery::Type::STOP_MAINTAIN
        || type == ASTSystemQuery::Type::ADD_REPLICA)
    {
        if (!payload->has("stream"))
        {
            error_msg = fmt::format("Missing stream name for command type {}", type_name);
            return false;
        }

        const auto & table = payload->get("stream").toString();
        if (table.empty())
        {
            error_msg = fmt::format("Stream name is empty for command type {}", type_name);
            return false;
        }
    }

    if (type == ASTSystemQuery::Type::ADD_REPLICA)
    {
        if (!payload->has("shard"))
        {
            error_msg = fmt::format("Missing shard number for command type {}", type_name);
            return false;
        }

        const auto shard = payload->get("shard").convert<Int32>();
        if (shard < 0)
        {
            error_msg = fmt::format("Missing or invalid shard number {} for {} command", shard, type_name);
            return false;
        }
    }
    else if (type == ASTSystemQuery::Type::REPLACE_REPLICA)
    {
        if (!payload->has("old_replica"))
        {
            error_msg = fmt::format("Missing 'old_replica' for {} command", type_name);
            return false;
        }

        const auto & old_replica = payload->get("old_replica").toString();
        if (old_replica.empty())
        {
            error_msg = fmt::format("Old replica name is empty for command type {}", type_name);
            return false;
        }
    }

    return true;
}

std::pair<String, Int32> SystemCommandHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & type = ASTSystemQuery::stringToType(payload->get("type").toString());
    const auto & name = payload->get("name").toString();

    String database;
    String table;

    if (payload->has("stream"))
    {
        table = payload->get("stream").toString();
        if (!payload->has("database"))
            database = query_context->getCurrentDatabase();
        else
            database = payload->get("database").toString();
    }

    if (!table.empty() && isDistributedDDL() && !CatalogService::instance(query_context).tableExists(database, table))
        return {
            jsonErrorResponse(fmt::format("Stream {}.{} does not exist.", database, table), ErrorCodes::UNKNOWN_STREAM),
            HTTPResponse::HTTP_BAD_REQUEST};

    /// construct query for system command
    String command_clause{ASTSystemQuery::typeToString(type)};
    String table_clause = table.empty() ? "" : fmt::format(" FROM TABLE `{}`.`{}`", database, table);

    if (type == ASTSystemQuery::Type::ADD_REPLICA)
        table_clause = fmt::format("{} SHARD {}", table_clause, payload->get("shard").toString());

    String query = fmt::format("SYSTEM {} '{}'{}", command_clause, name, table_clause);
    if (type == ASTSystemQuery::Type::REPLACE_REPLICA)
    {
        auto old_replica = payload->get("old_replica").toString();
        if (!CatalogService::instance(query_context).nodeByIdentity(old_replica))
            return {
                jsonErrorResponse(fmt::format("Replica {} does not exist.", old_replica), ErrorCodes::NO_REPLICA_NAME_GIVEN),
                HTTPResponse::HTTP_BAD_REQUEST};

        query = fmt::format("{} FOR '{}'", query, old_replica);
    }

    if (command_clause.empty())
        return {"", HTTPResponse::HTTP_BAD_REQUEST};

    setupDistributedQueryParameters({}, payload);

    LOG_INFO(log, "execute system command: {}", query);
    return {processQuery(query), HTTPResponse::HTTP_OK};
}

}
