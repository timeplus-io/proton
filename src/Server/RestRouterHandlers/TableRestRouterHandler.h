#pragma once

#include "RestRouterHandler.h"


namespace DB
{
class TableRestRouterHandler : public RestRouterHandler
{
public:
    TableRestRouterHandler(ContextPtr query_context_, const String & router_name) : RestRouterHandler(query_context_, router_name) { }
    ~TableRestRouterHandler() override { }

protected:
    static std::map<String, std::map<String, String>> update_schema;
    static std::map<String, String> granularity_func_mapping;

    static String getPartitionExpr(const Poco::JSON::Object::Ptr & payload, const String & default_granularity);
    static String getStringValueFrom(const Poco::JSON::Object::Ptr & payload, const String & key, const String & default_value);

    String buildResponse() const;
    String getEngineExpr(const Poco::JSON::Object::Ptr & payload) const;
    String processQuery(const String & query) const;
    String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const;

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executeDelete(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executePatch(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;

    virtual String getDefaultPartitionGranularity() const = 0;
    virtual String getDefaultOrderByGranularity() const = 0;
    virtual String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const = 0;
    virtual String getOrderByExpr(
        const Poco::JSON::Object::Ptr & payload, const String & time_column, const String & default_order_by_granularity) const = 0;
};

}
