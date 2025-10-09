#pragma once

#include <Server/RestRouterHandlers/RestRouterHandler.h>

namespace DB
{
class PythonStatusHandler final : public RestRouterHandler
{
public:
    explicit PythonStatusHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "PythonStatus") { }
    ~PythonStatusHandler() override { }

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
};
}
