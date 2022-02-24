#pragma once

#include "RestRouterHandler.h"

#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

namespace DB
{
class SQLFormatHandler final : public RestRouterHandler
{
public:
    explicit SQLFormatHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "SQLFormat") { }
    ~SQLFormatHandler() override = default;

    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
};
}
