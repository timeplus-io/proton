#pragma once

#include "TableRestRouterHandler.h"

namespace DB
{
class TabularTableRestRouterHandler final : public TableRestRouterHandler
{
public:
    explicit TabularTableRestRouterHandler(ContextPtr query_context_) : TableRestRouterHandler(query_context_, "Tabular") { }
    ~TabularTableRestRouterHandler() override { }

private:
    static std::map<String, std::map<String, String>> create_schema;
    static std::map<String, std::map<String, String>> column_schema;

    String getColumnDefinition(const Poco::JSON::Object::Ptr & column) const;

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    String getDefaultPartitionGranularity() const override;
    String getDefaultOrderByGranularity() const override;
    String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const override;
    String getOrderByExpr(
        const Poco::JSON::Object::Ptr & payload, const String & time_column, const String & default_order_by_granularity) const override;
};

}
