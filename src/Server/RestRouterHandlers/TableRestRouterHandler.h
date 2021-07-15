#pragma once

#include "RestRouterHandler.h"

#include <DistributedMetadata/CatalogService.h>

namespace DB
{
class ASTColumns;
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

    String getEngineExpr(const Poco::JSON::Object::Ptr & payload) const;

    /// Return empty string if the request is invalid
    String getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & shard) const;

    /// Return empty string if the setting is invalid
    String getAndValidateStorageSetting(const String & key) const;

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executeDelete(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePatch(const Poco::JSON::Object::Ptr & payload) const override;

    virtual void buildTablesJSON(Poco::JSON::Object & resp, const CatalogService::TablePtrs & tables) const = 0;
    virtual void buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_ast) const;
    virtual void buildTablePlacements(Poco::JSON::Object & resp_table, const String & table) const;

    virtual String getDefaultPartitionGranularity() const = 0;
    virtual String getDefaultOrderByGranularity() const = 0;
    virtual String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const = 0;
    virtual String getOrderByExpr(
        const Poco::JSON::Object::Ptr & payload, const String & time_column, const String & default_order_by_granularity) const = 0;
    virtual String subtype() const = 0;
};

}
