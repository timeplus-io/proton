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

bool RawstoreTableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(create_schema, payload, error_msg))
        return false;

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
    columns_definition.push_back("`_raw` string");
    columns_definition.push_back("`_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC (DoubleDelta, LZ4)");
    /// Let's not add `_tp_index_time` by default for now (internal users still can specify it in the payload
    columns_definition.push_back("`_tp_index_time` datetime64(3, 'UTC') DEFAULT 0 CODEC (DoubleDelta, LZ4)");
    columns_definition.push_back("`sourcetype` low_cardinality(string)");
    columns_definition.push_back("`source` string");
    columns_definition.push_back("`host` string");

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
