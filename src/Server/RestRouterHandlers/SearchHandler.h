#pragma once

#include "RestRouterHandler.h"

#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

namespace DB
{
class SearchHandler final : public RestRouterHandler
{
public:
    explicit SearchHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Search") { }
    ~SearchHandler() override = default;

    void execute(const Poco::JSON::Object::Ptr & payload, HTTPServerResponse & response) const override;

    bool streamingOutput() const override { return true; }

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String getQuery(const Poco::JSON::Object::Ptr & payload) const;

    std::shared_ptr<WriteBufferFromHTTPServerResponse> getOutputBuffer(HTTPServerResponse & response) const;
};
}
