#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class SystemCommandHandler final : public RestRouterHandler
{
public:
    explicit SystemCommandHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "System") { }
    ~SystemCommandHandler() override { }

protected:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;

private:
    static std::map<String, std::map<String, String>> command_schema;
};

}
