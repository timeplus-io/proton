#pragma once

#include <Server/RestRouterHandlers/TableRestRouterHandler.h>

namespace DB
{

/// REST handler for listing/inspecting objects created via `CREATE INPUT ...`.
/// The underlying object is stored as a table/stream; we detect inputs via `system.tables.is_input`.
class InputRestRouterHandler final : public TableRestRouterHandler
{
public:
    explicit InputRestRouterHandler(ContextMutablePtr query_context_) : TableRestRouterHandler(query_context_, "Input") { }
    ~InputRestRouterHandler() override = default;

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    void buildTablesJSON(Poco::JSON::Object & resp, const TablePtrs & tables) const override;

    String getDefaultPartitionGranularity() const override { return ""; }
    String getDefaultOrderByGranularity() const override { return ""; }
    String getColumnsDefinition(const Poco::JSON::Object::Ptr &) const override { return ""; }
    String getOrderByExpr(const Poco::JSON::Object::Ptr &, const String &, const String &) const override { return ""; }
    String subtype() const override { return "input"; }
};

}
