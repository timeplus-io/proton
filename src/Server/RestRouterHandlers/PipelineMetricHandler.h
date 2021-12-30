#pragma once

#include "RestRouterHandler.h"

#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

namespace DB
{
class PipelineMetricHandler final : public RestRouterHandler
{
public:
    explicit PipelineMetricHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "PipelineMetric") { }
    ~PipelineMetricHandler() override = default;

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
};
}
