#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class RestStatusHandler final : public RestRouterHandler
{
public:
    explicit RestStatusHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Status") { }
    ~RestStatusHandler() override { }

private:
    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    void buildResponse(const Block & block, String & resp) const;
};

}
