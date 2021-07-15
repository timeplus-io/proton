#include "RawstoreTableRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
std::map<String, std::map<String, String> > RawstoreTableRestRouterHandler::create_schema = {
    {"required",{
                        {"name","string"}
                }
    },
    {"optional", {
                        {"shards", "int"},
                        {"replication_factor", "int"},
                        {"order_by_granularity", "string"},
                        {"partition_by_granularity", "string"},
                        {"ttl_expression", "string"}
                }
    }
};

void RawstoreTableRestRouterHandler::buildTablesJSON(Poco::JSON::Object & resp, const CatalogService::TablePtrs & tables) const
{
    Poco::JSON::Array tables_mapping_json;
    std::unordered_set<String> table_names;

    for (const auto & table : tables)
    {
        if (table_names.contains(table->name))
            continue;

        /// FIXME : Later based on engine setting to distinguish rawstore
        if (table->create_table_query.find("`_raw` String COMMENT 'rawstore'") == String::npos)
        {
            continue;
        }

        const auto & query_ptr = parseQuery(table->create_table_query, query_context);
        const auto & create = query_ptr->as<const ASTCreateQuery &>();

        Poco::JSON::Object table_mapping_json;
        table_mapping_json.set("name", table->name);
        table_mapping_json.set("engine", table->engine);
        table_mapping_json.set("order_by_expression", table->sorting_key);
        table_mapping_json.set("partition_by_expression", table->partition_key);

        if (create.storage->ttl_table)
        {
            table_mapping_json.set("ttl", queryToString(*create.storage->ttl_table));
        }

        buildTablePlacements(table_mapping_json, table->name);
        tables_mapping_json.add(table_mapping_json);
        table_names.insert(table->name);
    }

    resp.set("data", tables_mapping_json);
}

bool RawstoreTableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(create_schema, payload, error_msg))
    {
        return false;
    }

    return TableRestRouterHandler::validatePost(payload, error_msg);
}

String RawstoreTableRestRouterHandler::getOrderByExpr(
    const Poco::JSON::Object::Ptr & payload, const String & /*time_column*/, const String & default_order_by_granularity) const
{
    const auto & order_by_granularity = getStringValueFrom(payload, "order_by_granularity", default_order_by_granularity);
    return granularity_func_mapping[order_by_granularity] + ", sourcetype";
}

String RawstoreTableRestRouterHandler::getColumnsDefinition(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    std::vector<String> columns_definition;
    columns_definition.push_back("`_raw` String COMMENT 'rawstore'");
    columns_definition.push_back("`_time` DateTime64(3) DEFAULT now64(3) CODEC (DoubleDelta, LZ4)");
    columns_definition.push_back("`_index_time` DateTime64(3) DEFAULT now64(3) CODEC (DoubleDelta, LZ4)");
    columns_definition.push_back("`sourcetype` LowCardinality(String)");
    columns_definition.push_back("`source` String");
    columns_definition.push_back("`host` String");

    return boost::algorithm::join(columns_definition, ",");
}

String RawstoreTableRestRouterHandler::getDefaultPartitionGranularity() const
{
    return "D";
}

String RawstoreTableRestRouterHandler::getDefaultOrderByGranularity() const
{
    return "m";
}

}
