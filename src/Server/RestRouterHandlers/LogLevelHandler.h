#pragma once

#include <Server/RestRouterHandlers/RestRouterHandler.h>

namespace DB
{

class LogLevelHandler final : public RestRouterHandler
{
public:
    explicit LogLevelHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "LogLevelHandler") { }

    ~LogLevelHandler() override = default;

    /// GET: Get current log levels
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    /// POST: Set log level for all loggers or specific logger
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
};
}
