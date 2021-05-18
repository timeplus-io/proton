#include "TableRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/queryToString.h>

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
    if (!query_context->isDistributed() || getQueryParameter("distributed") == "false")
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

String TableRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    const String & database_name = getPathParameter("database");
    const auto & database = DatabaseCatalog::instance().tryGetDatabase(database_name);

    if (!database)
    {
        http_status = HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse(fmt::format("Databases {} does not exist.", database_name), ErrorCodes::UNKNOWN_DATABASE);
    }

    const auto & catalog_service = CatalogService::instance(query_context);
    const auto & tables = catalog_service.findTableByDB(database_name);

    Poco::JSON::Object resp;
    buildTablesJSON(resp, tables);

    resp.set("query_id", query_context->getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    String resp_str = resp_str_stream.str();

    return resp_str;
}

String TableRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    const auto & database_name = getPathParameter("database");
    const auto & table_name = payload->get("name").toString();
    if (CatalogService::instance(query_context).tableExists(database_name, table_name))
    {
        http_status = HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse(fmt::format("Table {}.{} already exists.", database_name, table_name), ErrorCodes::TABLE_ALREADY_EXISTS);
    }

    const auto & shard = getQueryParameter("shard");
    const auto & query = getCreationSQL(payload, shard);

    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({}, payload);
    }

    return processQuery(query, query_context);
}

String TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");

    if (!CatalogService::instance(query_context).tableExists(database_name, table_name))
    {
        http_status = HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse(fmt::format("Table {}.{} doesn't exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);
    }

    LOG_INFO(log, "Updating table {}.{}", database_name, table_name);
    std::vector<String> create_segments;
    create_segments.push_back("ALTER TABLE " + database_name + "." + table_name);
    create_segments.push_back(" MODIFY TTL " + payload->get("ttl_expression").toString());

    const String & query = boost::algorithm::join(create_segments, " ");

    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({}, payload);
    }

    return processQuery(query, query_context);
}

String TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
{
    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");

    if (!CatalogService::instance(query_context).tableExists(database_name, table_name))
    {
        http_status = HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse(fmt::format("Table {}.{} doesn't exist", database_name, table_name), ErrorCodes::UNKNOWN_TABLE);
    }

    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({});
    }

    return processQuery("DROP TABLE " + database_name + "." + table_name, query_context);
}

void TableRestRouterHandler::buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list) const
{
    const auto & columns_ast = columns_list->columns;
    Poco::JSON::Array columns_mapping_json;
    for (auto ast_it = columns_ast->children.begin(); ast_it != columns_ast->children.end(); ++ast_it)
    {
        Poco::JSON::Object cloumn_mapping_json;

        const auto & col_decl = (*ast_it)->as<ASTColumnDeclaration &>();
        const auto & column_type = DataTypeFactory::instance().get(col_decl.type);

        cloumn_mapping_json.set("name", col_decl.name);
        
        String type = column_type->getName();
        if (column_type->isNullable())
        {
            type = removeNullable(column_type)->getName();
            cloumn_mapping_json.set("nullable", true);
        }
        else
        {
            cloumn_mapping_json.set("nullable", false);
        }
        cloumn_mapping_json.set("type", type);

        if (col_decl.default_expression)
        {
            cloumn_mapping_json.set("default", queryToString(col_decl.default_expression));
        }
        else
        {
            String alias = col_decl.tryGetAlias();
            if (!alias.empty())
            {
                cloumn_mapping_json.set("alias", alias);
            }
        }

        if (col_decl.comment)
        {
            cloumn_mapping_json.set("comment", queryToString(col_decl.comment));
        }

        if (col_decl.codec)
        {
            cloumn_mapping_json.set("codec", queryToString(col_decl.codec));
        }

        if (col_decl.ttl)
        {
            cloumn_mapping_json.set("ttl", queryToString(col_decl.ttl));
        }

        columns_mapping_json.add(cloumn_mapping_json);
    }
    resp_table.set("columns", columns_mapping_json);
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
    const auto & database_name = getPathParameter("database");
    const auto & time_col = getStringValueFrom(payload, "_time_column", "_time");
    std::vector<String> create_segments;
    create_segments.push_back("CREATE TABLE " + database_name + "." + payload->get("name").toString());
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

    if (!shard.empty())
    {
        create_segments.push_back("SETTINGS shard=" + shard);
    }

    return boost::algorithm::join(create_segments, " ");
}

}
