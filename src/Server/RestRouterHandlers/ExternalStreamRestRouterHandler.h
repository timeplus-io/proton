#pragma once

#include "TableRestRouterHandler.h"

#include <DistributedMetadata/CatalogService.h>

namespace DB
{
class ExternalStreamRestRouterHandler final : public TableRestRouterHandler
{
public:
    explicit ExternalStreamRestRouterHandler(ContextMutablePtr query_context_) : TableRestRouterHandler(query_context_, "ExternalStream") { }
    ~ExternalStreamRestRouterHandler() override { }

private:
    static std::map<String, std::map<String, String>> create_schema;
    static std::map<String, std::map<String, String>> column_schema;
    static std::map<String, std::map<String, String>> settings_schema;

    void buildTablesJSON(Poco::JSON::Object & resp, const CatalogService::TablePtrs & tables) const override;
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String getDefaultPartitionGranularity() const override { return ""; }
    String getDefaultOrderByGranularity() const override { return ""; }
    String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const override;
    String getSettings(const Poco::JSON::Object::Ptr & payload) const override;
    String getOrderByExpr(
        const Poco::JSON::Object::Ptr &, const String &, const String &) const override { return ""; }
    String subtype() const override { return "external_stream"; }
    bool isExternal() const override { return true; }
};

}
