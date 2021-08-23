#include "TableRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/queryToString.h>
#include <common/string.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
}

namespace
{
    const std::vector<String> CREATE_TABLE_SETTINGS = {
        "streaming_storage_cluster_id",
        "streaming_storage_subscription_mode",
        "streaming_storage_auto_offset_reset",
        "streaming_storage_request_required_acks",
        "streaming_storage_request_timeout_ms",
        "streaming_storage_retention_bytes",
        "streaming_storage_retention_ms",
        "streaming_storage_flush_messages",
        "streaming_storage_flush_ms",
        "distributed_ingest_mode",
        "distributed_flush_threshold_ms",
        "distributed_flush_threshold_count",
        "distributed_flush_threshold_bytes",
    };
}

std::map<String, std::map<String, String> > TableRestRouterHandler::update_schema = {
    {"required",{
                }
    },
    {"optional", {
                    {"ttl_expression", "string"}
                }
    }
};

std::map<String, String> TableRestRouterHandler::granularity_func_mapping = {
    {"M", "toYYYYMM(`_time`)"},
    {"D", "toYYYYMMDD(`_time`)"},
    {"H", "toStartOfHour(`_time`)"},
    {"m", "toStartOfMinute(`_time`)"}
};

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
    if (!query_context->isDistributed() || getQueryParameterBool("distributed", false))
    {
        int shards = payload->has("shards") ? payload->get("shards").convert<Int32>() : 1;
        int replication_factor = payload->has("replication_factor") ? payload->get("replication_factor").convert<Int32>() : 1;

        if (shards != 1 || replication_factor != 1)
        {
            error_msg = "Invalid shards / replication factor, local table shall have only 1 shard and 1 replica";
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
    {
        return {jsonErrorResponse(fmt::format("Databases {} does not exist.", database), ErrorCodes::UNKNOWN_DATABASE), HTTPResponse::HTTP_BAD_REQUEST};
    }

    const auto & catalog_service = CatalogService::instance(query_context);
    const auto & tables = catalog_service.findTableByDB(database);

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
    {
        return {
            jsonErrorResponse(fmt::format("Table {}.{} already exists.", database, table), ErrorCodes::TABLE_ALREADY_EXISTS),
            HTTPResponse::HTTP_BAD_REQUEST};
    }

    const auto & shard = getQueryParameter("shard");
    const auto & query = getCreationSQL(payload, shard);

    if (query.empty())
    {
        return {"", HTTPResponse::HTTP_BAD_REQUEST};
    }

    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({}, payload);
    }

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload) const
{
    const String & table = getPathParameter("table");

    if (isDistributedDDL() && !CatalogService::instance(query_context).tableExists(database, table))
    {
        return {
            jsonErrorResponse(fmt::format("Table {}.{} doesn't exist", database, table), ErrorCodes::UNKNOWN_TABLE),
            HTTPResponse::HTTP_BAD_REQUEST};
    }

    LOG_INFO(log, "Updating table {}.{}", database, table);
    std::vector<String> create_segments;
    create_segments.push_back("ALTER TABLE " + database + ".`" + table + "`");
    create_segments.push_back(" MODIFY TTL " + payload->get("ttl_expression").toString());

    const String & query = boost::algorithm::join(create_segments, " ");

    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({}, payload);
    }

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */) const
{
    const String & table = getPathParameter("table");

    if (isDistributedDDL() && !CatalogService::instance(query_context).tableExists(database, table))
    {
        return {
            jsonErrorResponse(fmt::format("Table {}.{} doesn't exist", database, table), ErrorCodes::UNKNOWN_TABLE),
            HTTPResponse::HTTP_BAD_REQUEST};
    }

    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({});
    }

    return {processQuery("DROP TABLE " + database + ".`" + table + "`"), HTTPResponse::HTTP_OK};
}

void TableRestRouterHandler::buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list) const
{
    const auto & columns_ast = columns_list->columns;
    Poco::JSON::Array columns_mapping_json;
    for (auto ast_it = columns_ast->children.begin(); ast_it != columns_ast->children.end(); ++ast_it)
    {
        Poco::JSON::Object column_mapping_json;

        const auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();
        const auto & column_type = DataTypeFactory::instance().get(col_decl.type);

        column_mapping_json.set("name", col_decl.name);

        String type = column_type->getName();
        if (column_type->isNullable())
        {
            type = removeNullable(column_type)->getName();
            column_mapping_json.set("nullable", true);
        }
        else
        {
            column_mapping_json.set("nullable", false);
        }
        column_mapping_json.set("type", type);

        if (col_decl.default_expression)
        {
            if (col_decl.default_specifier == "DEFAULT")
            {
                String default_str = queryToString(col_decl.default_expression);
                if (type == "String")
                {
                    default_str = default_str.substr(1, default_str.length() - 2);
                }

                column_mapping_json.set("default", default_str);
            }
            else if (col_decl.default_specifier == "ALIAS")
            {
                column_mapping_json.set("alias", queryToString(col_decl.default_expression));
            }
        }

        if (col_decl.comment)
        {
            const String & comment = queryToString(col_decl.comment);
            column_mapping_json.set("comment", comment.substr(1, comment.length() - 2));
        }

        if (col_decl.codec)
        {
            column_mapping_json.set("codec", queryToString(col_decl.codec));
        }

        if (col_decl.ttl)
        {
            column_mapping_json.set("ttl", queryToString(col_decl.ttl));
        }

        columns_mapping_json.add(column_mapping_json);
    }
    resp_table.set("columns", columns_mapping_json);
}

void TableRestRouterHandler::buildTablePlacements(Poco::JSON::Object & resp_table, const String & table) const
{
    const auto & catalog_service = CatalogService::instance(query_context);
    const auto & table_nodes = catalog_service.findTableByName(database, table);

    std::multimap<int, String> nodes;
    for (auto node : table_nodes)
    {
        nodes.emplace(node->shard, node->host);
    }

    Poco::JSON::Array shards;
    for (auto it = nodes.begin(); it != nodes.end(); it = nodes.upper_bound(it->first))
    {
        Poco::JSON::Object placement;
        placement.set("shard", it->first);

        auto range = nodes.equal_range(it->first);
        Poco::JSON::Array replicas;
        while (range.first != range.second)
        {
            replicas.add(range.first++->second);
        }
        placement.set("replicas", replicas);
        shards.add(placement);
    }
    resp_table.set("shards", shards);
}

String TableRestRouterHandler::getEngineExpr(const Poco::JSON::Object::Ptr & payload) const
{
    if (query_context->isDistributed())
    {
        if (getQueryParameter("distributed") != "false")
        {
            const auto & shards = getStringValueFrom(payload, "shards", "1");
            const auto & replication_factor = getStringValueFrom(payload, "replication_factor", "1");
            const auto & shard_by_expression = getStringValueFrom(payload, "shard_by_expression", "rand()");

            return fmt::format("DistributedMergeTree({}, {}, {})", replication_factor, shards, shard_by_expression);
        }
    }

    return "MergeTree()";
}

String TableRestRouterHandler::getPartitionExpr(const Poco::JSON::Object::Ptr & payload, const String & default_granularity)
{
    const auto & partition_by_granularity = getStringValueFrom(payload, "partition_by_granularity", default_granularity);
    return granularity_func_mapping[partition_by_granularity];
}

String TableRestRouterHandler::getStringValueFrom(const Poco::JSON::Object::Ptr & payload, const String & key, const String & default_value)
{
    return payload->has(key) ? payload->get(key).toString() : default_value;
}

String TableRestRouterHandler::getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const
{
    const auto & time_col = getStringValueFrom(payload, "_time_column", "_time");
    std::vector<String> create_segments;
    create_segments.push_back("CREATE TABLE " + database + ".`" + payload->get("name").toString() + "`");
    create_segments.push_back("(");
    create_segments.push_back(getColumnsDefinition(payload));
    create_segments.push_back(")");
    create_segments.push_back("ENGINE = " + getEngineExpr(payload));
    create_segments.push_back("PARTITION BY " + getPartitionExpr(payload, getDefaultPartitionGranularity()));
    create_segments.push_back("ORDER BY (" + getOrderByExpr(payload, time_col, getDefaultOrderByGranularity()) + ")");

    if (payload->has("ttl_expression"))
    {
        /// FIXME  Enforce time based TTL only
        create_segments.push_back("TTL " + payload->get("ttl_expression").toString());
    }

    create_segments.push_back("SETTINGS subtype='" + subtype() + "'");

    if (!shard.empty())
    {
        create_segments.push_back(", shard=" + shard);
    }

    for (const auto & key : CREATE_TABLE_SETTINGS)
    {
        if (hasQueryParameter(key))
        {
            auto result = getAndValidateStorageSetting(key);
            if (result.empty())
            {
                return "";
            }
            create_segments.push_back(", " + result);
        }
    }

    return boost::algorithm::join(create_segments, " ");
}

String TableRestRouterHandler::getAndValidateStorageSetting(const String & key) const
{
    const auto & value = getQueryParameter(key);

    if (value.empty())
    {
        return value;
    }

    if (key == "streaming_storage_subscription_mode")
    {
        if (value != "shared" && value != "dedicated")
        {
            return "";
        }

        return fmt::format("{}='{}'", key, value);
    }
    else if (key == "streaming_storage_auto_offset_reset")
    {
        if (value != "earliest" && value != "latest")
        {
            return "";
        }
        return fmt::format("{}='{}'", key, value);
    }
    else if (key == "distributed_ingest_mode")
    {
        if (value != "async" && value != "sync" && value != "fire_and_forget" && value != "ordered")
        {
            return "";
        }
        return fmt::format("{}='{}'", key, value);
    }
    else
    {
        return fmt::format("{}={}", key, value);
    }
}
}
