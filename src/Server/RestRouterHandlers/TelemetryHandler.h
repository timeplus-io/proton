#pragma once

#include "RestRouterHandler.h"

#include <Interpreters/Context.h>
#include "config.h"


namespace DB
{

class TelemetryHandler final : public RestRouterHandler
{
public:
    explicit TelemetryHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "TelemetryHandler") { }
    ~TelemetryHandler() override = default;

protected:
    static std::map<String, std::map<String, String>> update_schema;
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
};

}
