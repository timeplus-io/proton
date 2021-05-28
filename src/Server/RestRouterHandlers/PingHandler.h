#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class PingHandler final : public RestRouterHandler
{
public:
    explicit PingHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Status") { }
    ~PingHandler() override { }

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    void buildResponse(const Block & block, String & resp) const;
};

}
