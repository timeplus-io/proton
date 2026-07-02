#pragma once

#include <Server/RestRouterHandlers/RestRouterHandler.h>


namespace DB
{

class WriteBuffer;

class SearchHandler final : public RestRouterHandler
{
public:
    explicit SearchHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "Search") { }
    ~SearchHandler() override = default;

    void execute(const Poco::JSON::Object::Ptr & payload, HTTPServerResponse & response) const override;

    bool streamingOutput() const override { return true; }

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    String getQuery(const Poco::JSON::Object::Ptr & payload) const;

    std::unique_ptr<WriteBuffer> getOutputBuffer(HTTPServerResponse & response) const;

    void setQuerySettings() const;
};
}
