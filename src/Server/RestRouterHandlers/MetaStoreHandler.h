#pragma once

#include "config.h"

#if USE_NURAFT
#include <Interpreters/Context.h>
#include "RestRouterHandler.h"

namespace DB
{
class MetaStoreDispatcher;

class MetaStoreHandler final : public RestRouterHandler
{
public:
    explicit MetaStoreHandler(ContextMutablePtr query_context_)
        : RestRouterHandler(query_context_, "MetaStoreHandler"), metastore_dispatcher(query_context_->getMetaStoreDispatcher())
    {
    }
    ~MetaStoreHandler() override = default;

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executeDelete(const Poco::JSON::Object::Ptr & payload) const override;

    std::pair<String, Int32> doGet(const Poco::JSON::Object::Ptr & payload, const String & namespace_, const Strings & request_keys) const;
    std::pair<String, Int32>
    doList(const Poco::JSON::Object::Ptr & payload, const String & namespace_, const String & request_prefix) const;
    std::pair<String, Int32>
    doDelete(const Poco::JSON::Object::Ptr & payload, const String & namespace_, const std::vector<String> & keys) const;
    std::pair<String, Int32> doMultiGet(const Poco::JSON::Object::Ptr & payload, const String & namespace_) const;
    std::pair<String, Int32> doMultiDelete(const Poco::JSON::Object::Ptr & payload, const String & namespace_) const;

private:
    std::shared_ptr<MetaStoreDispatcher> metastore_dispatcher;
};

}

#endif
