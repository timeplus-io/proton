#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

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

    bool streamingInput() const override { return false; }

private:
    std::shared_ptr<MetaStoreDispatcher> metastore_dispatcher;
};

}

#endif
