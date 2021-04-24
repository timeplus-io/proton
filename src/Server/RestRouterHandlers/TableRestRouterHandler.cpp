#include "TableRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Core/Block.h>
#include <DistributedMetadata/CatalogService.h>
#include <Interpreters/executeQuery.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
    
namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
}

namespace
{

void buildColumnsJSON(Poco::JSON::Object & resp_table, const String & create_table_info)
{
    Poco::JSON::Array columns_mapping_json;

    String::size_type create_pos = String::npos;
    String::size_type engine_pos = String::npos;
    create_pos = create_table_info.find_first_of("(");
    engine_pos = create_table_info.find(") ENGINE");

    if (create_pos != String::npos && engine_pos != String::npos)
    {
        const String & columns_info = create_table_info.substr(create_pos + 1, engine_pos - create_pos - 1);

        Strings columns;
        boost::split(columns, columns_info, boost::is_any_of(","));
        for (auto & column : columns)
        {
            Poco::JSON::Object cloumn_mapping_json;

            int index = 0;
            Strings column_elements;
            boost::trim(column);
            boost::split(column_elements, column, boost::is_any_of(" "));
            cloumn_mapping_json.set("name", boost::algorithm::erase_all_copy(column_elements[index], "`"));

            int size = column_elements.size();

            /// extract type
            if (++index < size)
            {
                const String & type = column_elements[index];
                if (type.find("Nullable") != String::npos)
                {
                    cloumn_mapping_json.set("type", type.substr(type.find("(") + 1, type.find(")") - type.find("(") - 1));
                    cloumn_mapping_json.set("nullable", true);
                }
                else
                {
                    cloumn_mapping_json.set("type", type);
                    cloumn_mapping_json.set("nullable", false);
                }
            }

            /// extract default or alias
            if (++index < size)
            {
                const String & element = column_elements[index];
                if (element == "DEFAULT")
                {
                    cloumn_mapping_json.set("default", column_elements[++index]);
                }
                else if (element == "ALIAS")
                {
                    cloumn_mapping_json.set("alias", column_elements[++index]);
                }
                else
                {
                    index--;
                }
            }

            /// extract compression_codec
            if (++index < size)
            {
                const String & compression_codec = column_elements[index];

                if (compression_codec.find("CODEC") != String::npos)
                {
                    cloumn_mapping_json.set(
                        "compression_codec",
                        compression_codec.substr(
                            compression_codec.find("(") + 1, compression_codec.find(")") - compression_codec.find("(")));
                }
                else
                {
                    index--;
                }
            }

            /// FIXME : extract ttl_expression

            /// FIXME : extract skipping_index_expression

            columns_mapping_json.add(cloumn_mapping_json);
        }
    }

    resp_table.set("columns", columns_mapping_json);
}

void buildTablesJSON(Poco::JSON::Object & resp, const CatalogService::TablePtrs & tables)
{
    Poco::JSON::Array tables_mapping_json;

    for (const auto & table : tables)
    {
        /// FIXME : Later based on engin seting dstinguish table or rawstore
        if (table->create_table_query.find("`_raw` String COMMENT 'rawstore'") == String::npos)
        {
            Poco::JSON::Object table_mapping_json;

            table_mapping_json.set("name", table->name);
            table_mapping_json.set("engine", table->engine);
            table_mapping_json.set("order_by_expression", table->sorting_key);
            table_mapping_json.set("partition_by_expression", table->partition_key);
            table_mapping_json.set("_time_column", table->primary_key);

            /// extract ttl
            String::size_type ttl_pos = String::npos;
            ttl_pos = table->engine_full.find(" TTL ");
            if (ttl_pos != String::npos)
            {
                String::size_type settings_pos = String::npos;
                settings_pos = table->engine_full.find(" SETTINGS ");
                const String & ttl = table->engine_full.substr(ttl_pos + 5, settings_pos - ttl_pos - 5);
                table_mapping_json.set("ttl_expression", ttl);
            }

            buildColumnsJSON(table_mapping_json, table->create_table_query);
            tables_mapping_json.add(table_mapping_json);
        }
    }

    resp.set("data", tables_mapping_json);
}

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

String TableRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & /*http_status */) const
{
    const String & database_name = getPathParameter("database");
    const auto & database = DatabaseCatalog::instance().tryGetDatabase(database_name);

    if (!database)
    {
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

bool TableRestRouterHandler::validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(update_schema, payload, error_msg);
}

String TableRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & /*http_status*/) const
{
    const auto & shard = getQueryParameter("shard");
    const auto & query = getCreationSQL(payload, shard);

    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context->setQueryParameter("_payload", payload_str_stream.str());
        query_context->setDistributedDDLOperation(true);
    }

    return processQuery(query);
}

String TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & /*http_status*/) const
{
    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        query_context->setDistributedDDLOperation(true);
        query_context->setQueryParameter("_payload", "{}");
    }

    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");
    return processQuery("DROP TABLE " + database_name + "." + table_name);
}

String TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & /*http_status*/) const
{
    const String & database_name = getPathParameter("database");
    const String & table_name = getPathParameter("table");

    LOG_INFO(log, "Updating table {}.{}", database_name, table_name);
    std::vector<String> create_segments;
    create_segments.push_back("ALTER TABLE " + database_name + "." + table_name);
    create_segments.push_back(" MODIFY TTL " + payload->get("ttl_expression").toString());

    const String & query = boost::algorithm::join(create_segments, " ");

    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        query_context->setDistributedDDLOperation(true);

        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context->setQueryParameter("_payload", payload_str_stream.str());
    }

    return processQuery(query);
}

String TableRestRouterHandler::buildResponse() const
{
    Poco::JSON::Object resp;
    resp.set("query_id", query_context->getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

String TableRestRouterHandler::processQuery(const String & query) const
{
    BlockIO io{executeQuery(query, query_context, false /* internal */)};

    if (io.pipeline.initialized())
    {
        return "TableRestRouterHandler execute io.pipeline.initialized not implemented";
    }
    io.onFinish();

    return buildResponse();
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
