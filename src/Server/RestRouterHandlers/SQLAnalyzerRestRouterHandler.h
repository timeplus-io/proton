#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class SQLAnalyzerRestRouterHandler final : public RestRouterHandler
{
public:
    explicit SQLAnalyzerRestRouterHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "SQLAnalyzer") { }
    ~SQLAnalyzerRestRouterHandler() override { }

private:
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
};

}
