#include "RawstoreTableRestRouterHandler.h"
#include "SchemaValidator.h"

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
