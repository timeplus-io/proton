#pragma once

#include "TableRestRouterHandler.h"

namespace DB
{
class RawstoreTableRestRouterHandler final : public TableRestRouterHandler
{
public:
    explicit RawstoreTableRestRouterHandler(ContextPtr query_context_) : TableRestRouterHandler(query_context_, "RawStore")
    {
        query_context->setQueryParameter("table_type", "rawstore");
    }
    ~RawstoreTableRestRouterHandler() override { }

private:
    static std::map<String, std::map<String, String>> create_schema;

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String getDefaultPartitionGranularity() const override;
    String getDefaultOrderByGranularity() const override;
    String getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const override;
    String getOrderByExpr(
        const Poco::JSON::Object::Ptr & payload, const String & time_column, const String & default_order_by_granularity) const override;
};

}
