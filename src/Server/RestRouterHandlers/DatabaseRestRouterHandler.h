#pragma once

#include "RestRouterHandler.h"

#include <Processors/QueryPipeline.h>

namespace DB
{
class DatabaseRestRouterHandler final : public RestRouterHandler
{
public:
    explicit DatabaseRestRouterHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Database") { }
    ~DatabaseRestRouterHandler() override { }

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
    String executeDelete(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const override;

private:
    String processQuery(const String & query, Int32 & /* http_status */) const;
    String buildResponse() const;
    void processQueryWithProcessors(Poco::JSON::Object & resp, QueryPipeline & pipeline) const;
};

}
