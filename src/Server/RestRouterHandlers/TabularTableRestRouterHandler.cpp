#include "TabularTableRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <Common/ProtonCommon.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
}

std::map<String, std::map<String, String> > TabularTableRestRouterHandler::create_schema = {
    {"required",{
                    {"name","string"},
                    {"columns", "array"}
                }
    },
    {"optional", {
                    {"shards", "int"},
                    {RESERVED_EVENT_TIME_API_NAME, "string"},
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

    bool include_internal_streams = getQueryParameterBool("include_internal_streams", query_context->getSettingsRef().include_internal_streams.value);
    for (const auto & table : tables)
    {
        /// If include_internal_streams = false, ignore internal streams
        if (table->name.starts_with(".inner.") && !include_internal_streams)
            continue;

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

        if (create.storage && create.storage->ttl_table)
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
        return false;

    Poco::JSON::Array::Ptr columns = payload->getArray("columns");
    for (const auto & col : *columns)
    {
        const auto col_ptr = col.extract<Poco::JSON::Object::Ptr>();
        if (!validateSchema(column_schema, col_ptr, error_msg))
            return false;

        /// skip checking reserved columns for local mode
        if (!isDistributedDDL())
            continue;

        if (std::find(
                RESERVED_COLUMN_NAMES.begin(), RESERVED_COLUMN_NAMES.end(), col_ptr->get("name").toString())
            != RESERVED_COLUMN_NAMES.end())
        {
            error_msg = "Column '" + col_ptr->get("name").toString() + "' is reserved.";
            return false;
        }

        if (std::find(
                RESERVED_COLUMN_NAMES.begin(), RESERVED_COLUMN_NAMES.end(), col.extract<Poco::JSON::Object::Ptr>()->get("name").toString())
            != RESERVED_COLUMN_NAMES.end())
        {
            error_msg = "Column '" + col.extract<Poco::JSON::Object::Ptr>()->get("name").toString() + "' is reserved.";
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
        columns_definition.push_back(getCreateColumnDefination(col.extract<Poco::JSON::Object::Ptr>()));

    if (isDistributedDDL())
    {
        if (payload->has(RESERVED_EVENT_TIME_API_NAME))
        {
            /// FIXME: validate the result type of RESERVED_EVENT_TIME_API_NAME expression
            columns_definition.push_back(
                "`" + RESERVED_EVENT_TIME + "` DateTime64(3, 'UTC') DEFAULT " + payload->get(RESERVED_EVENT_TIME_API_NAME).toString());
        }
        else
        {
            columns_definition.push_back("`" + RESERVED_EVENT_TIME + "` DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC (DoubleDelta, LZ4)");
        }
        /// RESERVED_INDEX_TIME will need recalculate when the block gets indexed to historical store
        columns_definition.push_back("`" + RESERVED_INDEX_TIME + "` DateTime64(3, 'UTC') CODEC (DoubleDelta, LZ4)");
        /// RESERVED_EVENT_SEQUENCE_ID will be recalculate when the block gets indexed to historical store
        /// columns_definition.push_back("`" + RESERVED_EVENT_SEQUENCE_ID + "` Int64 CODEC (Delta, LZ4)");
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
