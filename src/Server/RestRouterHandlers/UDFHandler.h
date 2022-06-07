#pragma once

#include "RestRouterHandler.h"

#include <Interpreters/Context.h>
#include <Common/config.h>
#include "config_core.h"


namespace DB
{
class MetaStoreDispatcher;

class UDFHandler final : public RestRouterHandler
{
public:
    explicit UDFHandler(ContextMutablePtr query_context_)
        : RestRouterHandler(query_context_, "UDFHandler"), metastore_repo(query_context_->getMetaStoreJSONConfigRepository())
    {
    }
    ~UDFHandler() override = default;

protected:
    static std::map<String, std::map<String, String>> create_schema;
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executeDelete(const Poco::JSON::Object::Ptr & payload) const override;

    std::pair<String, Int32> doGet(const Poco::JSON::Object::Ptr & payload, const String & namespace_, const Strings & request_keys) const;
    std::pair<String, Int32>
    doList(const Poco::JSON::Object::Ptr & payload, const String & namespace_, const String & request_prefix) const;
    bool streamingInput() const override { return false; }

private:
    MetaStoreJSONConfigRepository * metastore_repo;
};

}
