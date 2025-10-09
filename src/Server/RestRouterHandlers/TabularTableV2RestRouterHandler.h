#pragma once

#include <Server/RestRouterHandlers/TabularTableRestRouterHandler.h>

namespace DB
{
class TabularTableV2RestRouterHandler final : public TableRestRouterHandler
{
public:
    explicit TabularTableV2RestRouterHandler(ContextMutablePtr context) : TableRestRouterHandler(context, "TabularV2"), v1_handler(context)
    {
    }
    ~TabularTableV2RestRouterHandler() override = default;

private:
    void buildTablesJSON(Poco::JSON::Object & resp, const TablePtrs & tables) const override;

    bool validateGet(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validateDelete(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String getDefaultPartitionGranularity() const override { return v1_handler.getDefaultPartitionGranularity(); }
    String getDefaultOrderByGranularity() const override { return v1_handler.getDefaultOrderByGranularity(); }
    String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const override { return v1_handler.getColumnsDefinition(payload); }
    String getOrderByExpr(
        const Poco::JSON::Object::Ptr & payload, const String & time_column, const String & default_order_by_granularity) const override
    {
        return v1_handler.getOrderByExpr(payload, time_column, default_order_by_granularity);
    }

    String subtype() const override { return "tabularv2"; }

    inline bool validateDatabase(String & error_msg) const;

private:
    mutable TabularTableRestRouterHandler v1_handler;
};

}
