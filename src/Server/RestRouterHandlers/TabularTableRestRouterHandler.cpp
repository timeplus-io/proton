#include "TabularTableRestRouterHandler.h"
#include "SchemaValidator.h"

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
std::map<String, std::map<String, String> > TabularTableRestRouterHandler::create_schema= {
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
                    {"compression_codec", "string"},
                    {"ttl_expression", "string"},
                    {"skipping_index_expression", "string"}
                }
    }
};

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
        columns_definition.push_back(getColumnDefinition(col.extract<Poco::JSON::Object::Ptr>()));
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

String TabularTableRestRouterHandler::getColumnDefinition(const Poco::JSON::Object::Ptr & column) const
{
    std::vector<String> column_definition;

    column_definition.push_back(column->get("name").toString());
    if (column->has("nullable") && column->get("nullable"))
    {
        column_definition.push_back(" Nullable(" + column->get("type").toString() + ")");
    }
    else
    {
        column_definition.push_back(" " + column->get("type").toString());
    }

    if (column->has("default"))
    {
        column_definition.push_back(" DEFAULT " + column->get("default").toString());
    }

    if (column->has("compression_codec"))
    {
        column_definition.push_back(" CODEC(" + column->get("compression_codec").toString() + ")");
    }

    if (column->has("ttl_expression"))
    {
        column_definition.push_back(" TTL " + column->get("ttl_expression").toString());
    }

    if (column->has("skipping_index_expression"))
    {
        column_definition.push_back(", " + column->get("skipping_index_expression").toString());
    }

    return boost::algorithm::join(column_definition, " ");
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
