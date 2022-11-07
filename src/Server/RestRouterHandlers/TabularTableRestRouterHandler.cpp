#include "TabularTableRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <boost/algorithm/string/join.hpp>
#include <Common/ProtonCommon.h>

#include <vector>

namespace DB
{
std::map<String, std::map<String, String>> TabularTableRestRouterHandler::create_schema
    = {{"required", {{"name", "string"}, {"columns", "array"}}},
       {"optional",
        {{"shards", "int"},
         {ProtonConsts::RESERVED_EVENT_TIME_API_NAME, "string"},
         {"replication_factor", "int"},
         {"order_by_expression", "string"},
         {"order_by_granularity", "string"},
         {"primary_key", "string"},
         {"partition_by_expression", "string"},
         {"partition_by_granularity", "string"},
         {"ttl_expression", "string"},
         {"mode", "string"}}}};

std::map<String, std::map<String, String>> TabularTableRestRouterHandler::column_schema
    = {{"required",
        {
            {"name", "string"},
            {"type", "string"},
        }},
       {"optional",
        {{"nullable", "bool"},
         {"default", "string"},
         {"alias", "string"},
         {"compression_codec", "string"},
         {"ttl_expression", "string"},
         {"skipping_index_expression", "string"}}}};

void TabularTableRestRouterHandler::buildTablesJSON(Poco::JSON::Object & resp, const CatalogService::TablePtrs & tables) const
{
    Poco::JSON::Array tables_mapping_json;
    std::unordered_set<String> table_names;

    bool include_internal_streams
        = getQueryParameterBool("include_internal_streams", query_context->getSettingsRef().include_internal_streams.value);

    for (const auto & table : tables)
    {
        /// If include_internal_streams = false, ignore internal streams
        if (table->name.starts_with(".inner.") && !include_internal_streams)
            continue;

        if (table_names.contains(table->name))
            continue;

        if (table->engine_full.find("subtype = 'rawstore'") != String::npos)
            continue;

        if (table->create_table_query.empty())
            continue;

        const auto & query_ptr = parseQuery(table->create_table_query, query_context);
        const auto & create = query_ptr->as<const ASTCreateQuery &>();

        Poco::JSON::Object table_mapping_json;
        table_mapping_json.set("name", table->name);
        table_mapping_json.set("engine", table->engine);
        table_mapping_json.set("order_by_expression", table->sorting_key);
        table_mapping_json.set("partition_by_expression", table->partition_key);

        if (table->engine == "Stream" || table->engine == "MaterializedView")
            buildRetentionSettings(table_mapping_json, table->database.empty() ? database : table->database, table->name);

        if (create.storage && create.storage->ttl_table)
            table_mapping_json.set("ttl", queryToString(*create.storage->ttl_table));

        buildColumnsJSON(table_mapping_json, create.columns_list);
        buildTablePlacements(table_mapping_json, table->name);
        tables_mapping_json.add(table_mapping_json);

        table_names.insert(table->name);
    }

    resp.set("data", tables_mapping_json);
}

bool TabularTableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(create_schema, payload, error_msg))
        return false;

    Poco::JSON::Array::Ptr columns = payload->getArray("columns");
    for (const auto & col : *columns)
    {
        const auto col_ptr = col.extract<Poco::JSON::Object::Ptr>();
        if (!validateSchema(column_schema, col_ptr, error_msg))
            return false;

        if (!isDistributedDDL())
            continue;

        if (std::find(ProtonConsts::RESERVED_COLUMN_NAMES.begin(), ProtonConsts::RESERVED_COLUMN_NAMES.end(), col_ptr->get("name").toString())
            != ProtonConsts::RESERVED_COLUMN_NAMES.end())
        {
            error_msg = fmt::format("Column '{}' is reserved.", col_ptr->get("name").toString());
            return false;
        }

        if (std::find(
                ProtonConsts::RESERVED_COLUMN_NAMES.begin(), ProtonConsts::RESERVED_COLUMN_NAMES.end(), col.extract<Poco::JSON::Object::Ptr>()->get("name").toString())
            != ProtonConsts::RESERVED_COLUMN_NAMES.end())
        {
            error_msg = fmt::format("Column '{}' is reserved.", col.extract<Poco::JSON::Object::Ptr>()->get("name").toString());
            return false;
        }
    }

    const auto & storage_mode = payload->has("mode") ? payload->get("mode").toString() : ProtonConsts::APPEND_MODE;

    if (storage_mode != ProtonConsts::APPEND_MODE && storage_mode != ProtonConsts::CHANGELOG_MODE
        && storage_mode != ProtonConsts::CHANGELOG_KV_MODE && storage_mode != ProtonConsts::VERSIONED_KV_MODE)
    {
        error_msg = "Storage 'mode' can be only 'append', 'changelog', 'changelog_kv' or 'versioned_kv'";
        return false;
    }

    if (storage_mode == ProtonConsts::CHANGELOG_KV_MODE || storage_mode == ProtonConsts::VERSIONED_KV_MODE)
    {
        if (!payload->has("primary_key"))
        {
            error_msg = fmt::format("changelog kv or versioned kv storage mode requires 'PRIMARY KEY'");
            return false;
        }

        if (payload->has("order_by_expression"))
        {
            if (payload->get("primary_key").toString() != payload->get("order_by_expression").toString())
            {
                error_msg = fmt::format("changelog kv or versioned kv storage mode requires 'ORDER BY' is the same as 'PRIMARY KEY'");
                return false;
            }
        }
    }

    return TableRestRouterHandler::validatePost(payload, error_msg);
}

String TabularTableRestRouterHandler::getOrderByExpr(
    const Poco::JSON::Object::Ptr & payload, const String & /*time_column*/, const String & default_order_by_granularity) const
{
    /// If order_by_expression is explicitly specified, honor it
    const auto & order_by_expression = getStringValueFrom(payload, "order_by_expression", String());
    if (!order_by_expression.empty())
        return order_by_expression;

    /// If primary key is explicitly specified, use it for order by
    const auto & primary_key = getStringValueFrom(payload, "primary_key", String());
    if (!primary_key.empty())
        return primary_key;

    const auto & order_by_granularity = getStringValueFrom(payload, "order_by_granularity", default_order_by_granularity);
    return granularity_func_mapping[order_by_granularity];
}

String TabularTableRestRouterHandler::getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & columns = payload->getArray("columns");

    bool has_event_time = false, has_index_time = false, has_delta_flag = false;
    std::vector<String> columns_definition;
    for (const auto & col : *columns)
    {
        auto col_json{col.extract<Poco::JSON::Object::Ptr>()};
        columns_definition.push_back(getCreateColumnDefinition(col_json));

        const auto & name = col_json->get("name").toString();
        if (name == ProtonConsts::RESERVED_EVENT_TIME)
            has_event_time = true;
        else if (name == ProtonConsts::RESERVED_INDEX_TIME)
            has_index_time = true;
        else if (name == ProtonConsts::RESERVED_DELTA_FLAG)
            has_delta_flag = true;
    }

    if (!has_event_time)
    {
        if (payload->has(ProtonConsts::RESERVED_EVENT_TIME_API_NAME))
            /// FIXME: validate the result type of RESERVED_EVENT_TIME_API_NAME expression
            columns_definition.push_back(fmt::format(
                "`{}` datetime64(3, 'UTC') DEFAULT {}", ProtonConsts::RESERVED_EVENT_TIME, payload->get(ProtonConsts::RESERVED_EVENT_TIME_API_NAME).toString()));
        else
            columns_definition.push_back(
                fmt::format("`{}` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC (DoubleDelta, LZ4)", ProtonConsts::RESERVED_EVENT_TIME));
    }

    if (!has_index_time)
        /// RESERVED_INDEX_TIME will need recalculate when the block gets indexed to historical store
        columns_definition.push_back(fmt::format("`{}` datetime64(3, 'UTC') CODEC (DoubleDelta, LZ4)", ProtonConsts::RESERVED_INDEX_TIME));

    if (!has_delta_flag)
    {
        const auto & storage_mode = payload->has("mode") ? payload->get("mode").toString() : ProtonConsts::APPEND_MODE;
        if (storage_mode == ProtonConsts::CHANGELOG_MODE || storage_mode == ProtonConsts::CHANGELOG_KV_MODE)
            columns_definition.push_back(fmt::format("`{}` int8 DEFAULT 1", ProtonConsts::RESERVED_DELTA_FLAG));
    }

    /// RESERVED_EVENT_SEQUENCE_ID will be recalculate when the block gets indexed to historical store
    /// columns_definition.push_back("`" + RESERVED_EVENT_SEQUENCE_ID + "` Int64 CODEC (Delta, LZ4)");
    return boost::algorithm::join(columns_definition, ", ");
}

String TabularTableRestRouterHandler::getDefaultPartitionGranularity() const
{
    return "M";
}

String TabularTableRestRouterHandler::getDefaultOrderByGranularity() const
{
    return "D";
}

}
