#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <Server/RestRouterHandlers/RestRouterHandler.h>

namespace DB
{
class PythonPackageHandler final : public RestRouterHandler
{
public:
    explicit PythonPackageHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "python_package") { }
    ~PythonPackageHandler() override { }

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executeDelete(const Poco::JSON::Object::Ptr & payload) const override;
};

}

#endif
