#include "TabularTableRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
std::map<String, std::map<String, String> > TabularTableRestRouterHandler::create_schema = {
    {"required",{
                    {"name","string"},
                    {"columns", "array"}
                }
    },
    {"optional", {
                    {"shards", "int"},
                    {"_time_column", "string"},
                    {"replication_factor", "int"},
                    {"order_by_expression", "string"},
                    {"order_by_granularity", "string"},
                    {"partition_by_granularity", "string"},
                    {"ttl_expression", "string"}
                }
    }
};

std::map<String, std::map<String, String> > TabularTableRestRouterHandler::column_schema = {
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

void TabularTableRestRouterHandler::buildTablesJSON(Poco::JSON::Object & resp, const CatalogService::TablePtrs & tables) const
{
    Poco::JSON::Array tables_mapping_json;
    std::unordered_set<String> table_names;

    for (const auto & table : tables)
    {
        if (table_names.contains(table->name))
            continue;

        if (table->engine_full.find("subtype = 'rawstore'") != String::npos)
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
    {
        return false;
    }

    Poco::JSON::Array::Ptr columns = payload->getArray("columns");
    for (const auto & col : *columns)
    {
        if (!validateSchema(column_schema, col.extract<Poco::JSON::Object::Ptr>(), error_msg))
        {
            return false;
        }
    }

    return TableRestRouterHandler::validatePost(payload, error_msg);
}

String TabularTableRestRouterHandler::getOrderByExpr(
    const Poco::JSON::Object::Ptr & payload, const String & /*time_column*/, const String & default_order_by_granularity) const
{
    const auto & order_by_granularity = getStringValueFrom(payload, "order_by_granularity", default_order_by_granularity);
    const auto & default_order_expr = granularity_func_mapping[order_by_granularity];
    const auto & order_by_expression = getStringValueFrom(payload, "order_by_expression", String());

    if (order_by_expression.empty())
    {
        return default_order_expr;
    }

    /// FIXME: We may need to check whether the time column is already set as the first column in order by expression.

    return default_order_expr + ", " + order_by_expression;
}

String TabularTableRestRouterHandler::getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & columns = payload->getArray("columns");

    std::vector<String> columns_definition;
    for (const auto & col : *columns)
    {
        columns_definition.push_back(getCreateColumnDefination(col.extract<Poco::JSON::Object::Ptr>()));
    }

    if (payload->has("_time_column"))
    {
        columns_definition.push_back("`_time` DateTime64(3) DEFAULT " + payload->get("_time_column").toString());
    }
    else
    {
        columns_definition.push_back("`_time` DateTime64(3, UTC) DEFAULT now64(3)");
    }

    return boost::algorithm::join(columns_definition, ",");
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
