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
    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    void buildResponse(const Block & block, String & resp) const;
};

}
