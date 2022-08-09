#include "TableRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <DataTypes/DataTypeNullable.h>
#include <DistributedMetadata/queryStreams.h>
#include <Interpreters/Streaming/ASTToJSONUtils.h>
#include <Interpreters/Streaming/DDLHelper.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Common/ProtonCommon.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int STREAM_ALREADY_EXISTS;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_STREAM;
}

namespace
{
    enum class DeleteMode
    {
        TRUNCATE,
        DROP,
        INVALID,
    };

    inline DeleteMode toDeleteMode(const std::string & mode)
    {
        if (mode == "truncate")
            return DeleteMode::TRUNCATE;
        else if (mode == "drop")
            return DeleteMode::DROP;
        else
            return DeleteMode::INVALID;
    }
}

std::map<String, std::map<String, String>> TableRestRouterHandler::update_schema
    = {{"required", {}}, {"optional", {{"ttl_expression", "string"}}}};

std::map<String, String> TableRestRouterHandler::granularity_func_mapping
    = {{"M", "to_YYYYMM(`" + ProtonConsts::RESERVED_EVENT_TIME + "`)"},
       {"D", "to_YYYYMMDD(`" + ProtonConsts::RESERVED_EVENT_TIME + "`)"},
       {"H", "to_start_of_hour(`" + ProtonConsts::RESERVED_EVENT_TIME + "`)"},
       {"m", "to_start_of_minute(`" + ProtonConsts::RESERVED_EVENT_TIME + "`)"}};

bool TableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (payload->has("partition_by_granularity"))
    {
        if (!granularity_func_mapping.contains(payload->get("partition_by_granularity").toString()))
        {
            error_msg = "Invalid partition_by_granularity, only `m, H, D, M` are supported";
            return false;
        }
    }

    if (payload->has("order_by_granularity"))
    {
        if (!granularity_func_mapping.contains(payload->get("order_by_granularity").toString()))
        {
            error_msg = "Invalid order_by_granularity, only `m, H, D, M` are supported";
            return false;
        }
    }

    /// For non-distributed env or user force to create a `local` MergeTree table
    if (!query_context->isDistributedEnv() || getQueryParameterBool("distributed", false))
    {
        int shards = payload->has("shards") ? payload->get("shards").convert<Int32>() : 1;
        int replication_factor = payload->has("replication_factor") ? payload->get("replication_factor").convert<Int32>() : 1;

        if (shards != 1 || replication_factor != 1)
        {
            error_msg = "Invalid shards / replication factor, local stream shall have only 1 shard and 1 replica";
            return false;
        }
    }

    return true;
}

bool TableRestRouterHandler::validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(update_schema, payload, error_msg);
}

std::pair<String, Int32> TableRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
{
    if (!DatabaseCatalog::instance().tryGetDatabase(database))
        return {
            jsonErrorResponse(fmt::format("Databases {} does not exist.", database), ErrorCodes::UNKNOWN_DATABASE),
            HTTPResponse::HTTP_BAD_REQUEST};

    CatalogService::TablePtrs tables;
    if (!CatalogService::instance(query_context).enabled())
    {
        auto node_identity{query_context->getNodeIdentity()};
        auto this_host{query_context->getHostFQDN()};
        queryStreams(query_context, [&](Block && block) {
            tables.reserve(block.rows());
            for (size_t row = 0; row < block.rows(); ++row)
                tables.push_back(std::make_shared<CatalogService::Table>(node_identity, this_host, block, row));
        });
    }
    else
    {
        const auto & catalog_service = CatalogService::instance(query_context);
        auto database_tables{catalog_service.findTableByDB(database)};
        tables.swap(database_tables);
    }

    Poco::JSON::Object resp;
    buildTablesJSON(resp, tables);

    resp.set("request_id", query_context->getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    String resp_str = resp_str_stream.str();

    return {resp_str, HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> TableRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & table = payload->get("name").toString();
    /// Only check table existence when the ddl is distributed since when it is local, the creation
    /// may already happen in other nodes and broadcast to the action node, in this case, we will
    /// report table exist failure but we should not
    if (isDistributedDDL() && CatalogService::instance(query_context).tableExists(database, table))
        return {
            jsonErrorResponse(fmt::format("Stream {}.{} already exists.", database, table), ErrorCodes::STREAM_ALREADY_EXISTS),
            HTTPResponse::HTTP_BAD_REQUEST};

    const auto & host_shards = getQueryParameter("host_shards");
    const auto & uuid = getQueryParameter("uuid");
    const auto & synchronous_ddl = getQueryParameter("synchronous_ddl", "1");
    const auto & query = getCreationSQL(payload, host_shards, uuid);

    if (synchronous_ddl == "1")
        query_context->setSetting("synchronous_ddl", true);
    else
        query_context->setSetting("synchronous_ddl", false);

    if (query.empty())
        return {"", HTTPResponse::HTTP_BAD_REQUEST};

    setupDistributedQueryParameters({}, payload);

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload) const
{
    const String & table = getPathParameter("stream");

    if (isDistributedDDL() && !CatalogService::instance(query_context).tableExists(database, table))
    {
        return {
            jsonErrorResponse(fmt::format("Stream {}.{} doesn't exist", database, table), ErrorCodes::UNKNOWN_STREAM),
            HTTPResponse::HTTP_BAD_REQUEST};
    }

    LOG_INFO(log, "Updating stream {}.{}", database, table);
    std::vector<String> create_segments;
    create_segments.push_back("ALTER STREAM " + database + ".`" + table + "`");
    create_segments.push_back(" MODIFY TTL " + payload->get("ttl_expression").toString());

    const String & query = boost::algorithm::join(create_segments, " ");

    setupDistributedQueryParameters({}, payload);

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */) const
{
    const String & table = getPathParameter("stream");

    String query = fmt::format("DROP STREAM {}.`{}`", database, table);
    if (hasQueryParameter("mode"))
    {
        const auto & mode = getQueryParameter("mode");
        auto delete_mode = toDeleteMode(mode);
        if (delete_mode == DeleteMode::INVALID)
            return {
                jsonErrorResponse("No support delete mode: " + mode, ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};
        else if (delete_mode == DeleteMode::TRUNCATE)
            query = "TRUNCATE STREAM " + database + ".`" + table + "`";
    }

    if (isDistributedDDL() && !CatalogService::instance(query_context).tableExists(database, table))
    {
        return {
            jsonErrorResponse(fmt::format("Stream {}.{} doesn't exist", database, table), ErrorCodes::UNKNOWN_STREAM),
            HTTPResponse::HTTP_BAD_REQUEST};
    }

    setupDistributedQueryParameters({});

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

void TableRestRouterHandler::buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list) const
{
    const auto & columns_ast = columns_list->columns;
    Poco::JSON::Array columns_mapping_json;
    for (auto & ast_it : columns_ast->children)
    {
        Poco::JSON::Object column_mapping_json;
        const auto & col_decl = ast_it->as<ASTColumnDeclaration &>();

        ColumnDeclarationToJSON(column_mapping_json, col_decl);
        columns_mapping_json.add(column_mapping_json);
    }
    resp_table.set("columns", columns_mapping_json);
}

void TableRestRouterHandler::buildTablePlacements(Poco::JSON::Object & resp_table, const String & table) const
{
    const auto & catalog_service = CatalogService::instance(query_context);
    const auto & table_nodes = catalog_service.findTableByName(database, table);

    std::multimap<int, String> nodes;
    for (const auto node : table_nodes)
        for (auto shard : node->host_shards)
            nodes.emplace(shard, node->host);

    Poco::JSON::Array shards;
    for (auto it = nodes.begin(); it != nodes.end(); it = nodes.upper_bound(it->first))
    {
        Poco::JSON::Object placement;
        placement.set("shard", it->first);

        auto range = nodes.equal_range(it->first);
        Poco::JSON::Array replicas;
        while (range.first != range.second)
            replicas.add(range.first++->second);

        placement.set("replicas", replicas);
        shards.add(placement);
    }
    resp_table.set("shards", shards);
}

String TableRestRouterHandler::getEngineExpr(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & shards = getStringValueFrom(payload, "shards", "1");
    const auto & replication_factor = getStringValueFrom(payload, "replication_factor", "1");
    const auto & shard_by_expression = getStringValueFrom(payload, "shard_by_expression", "rand()");

    return fmt::format("Stream({}, {}, {})", shards, replication_factor, shard_by_expression);
}

String TableRestRouterHandler::getPartitionExpr(const Poco::JSON::Object::Ptr & payload, const String & default_granularity)
{
    /// If `partition by` is explicitly specified, honor it
    const auto & partition_by_expression = getStringValueFrom(payload, "partition_by_expression", String());
    if (!partition_by_expression.empty())
        return partition_by_expression;

    const auto & partition_by_granularity = getStringValueFrom(payload, "partition_by_granularity", default_granularity);
    return granularity_func_mapping[partition_by_granularity];
}

String TableRestRouterHandler::getStringValueFrom(const Poco::JSON::Object::Ptr & payload, const String & key, const String & default_value)
{
    return payload->has(key) ? payload->get(key).toString() : default_value;
}

String
TableRestRouterHandler::getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & host_shards, const String & uuid) const
{
    const auto & time_col = getStringValueFrom(payload, ProtonConsts::RESERVED_EVENT_TIME_API_NAME, ProtonConsts::RESERVED_EVENT_TIME);
    std::vector<String> create_segments;

    if (isExternal())
    {
        return fmt::format(
            "CREATE EXTERNAL STREAM `{}`.`{}` ({}) SETTINGS {}",
            database,
            payload->get("name").toString(),
            getColumnsDefinition(payload),
            getSettings(payload));
    }

    const auto & order_by = getOrderByExpr(payload, time_col, getDefaultOrderByGranularity());
    const auto & primary_key = payload->has("primary_key") ? payload->get("primary_key").toString() : order_by;
    const auto & partition_by = getPartitionExpr(payload, getDefaultPartitionGranularity());

    if (uuid.empty())
    {
        create_segments.push_back(fmt::format(
            "CREATE STREAM `{}`.`{}` ({}) ENGINE = {} PARTITION BY {} PRIMARY KEY ({}) ORDER BY ({})",
            database,
            payload->get("name").toString(),
            getColumnsDefinition(payload),
            getEngineExpr(payload),
            partition_by,
            primary_key,
            order_by));
    }
    else
    {
        create_segments.push_back(fmt::format(
            "CREATE STREAM `{}`.`{}` UUID '{}' ({}) ENGINE = {} PARTITION BY {} PRIMARY KEY ({}) ORDER BY ({})",
            database,
            payload->get("name").toString(),
            uuid,
            getColumnsDefinition(payload),
            getEngineExpr(payload),
            partition_by,
            primary_key,
            order_by));
    }

    if (payload->has("ttl_expression"))
        /// FIXME  Enforce time based TTL only
        create_segments.push_back(fmt::format("TTL {}", payload->get("ttl_expression").toString()));

    create_segments.push_back(fmt::format("SETTINGS subtype='{}'", subtype()));

    if (payload->has("mode"))
        create_segments.push_back(fmt::format(", mode='{}'", payload->get("mode").toString()));

    if (!host_shards.empty())
        create_segments.push_back(fmt::format(", host_shards='{}'", host_shards));

    getAndValidateStorageSetting(
        [this](const auto & key) -> String {
            if (hasQueryParameter(key))
                return getQueryParameter(key);

            return "";
        },
        [&](const auto & key, const auto & value) {
            if (key != "subtype" && key != "mode")
                create_segments.push_back(fmt::format(", {}='{}'", key, value));
        });

    return boost::algorithm::join(create_segments, " ");
}

}
