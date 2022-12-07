#pragma once

#include "RestRouterHandler.h"

#include <Interpreters/Context.h>

namespace DB
{
/// Get bytes on disk for streams, contains:
/// 1) data of streaming store (NativeLog)
/// 2) data of historical store (Storage)
class StorageInfoHandler final : public RestRouterHandler
{
public:
    explicit StorageInfoHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "StorageInfoHandler") { }
    ~StorageInfoHandler() override = default;

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
};

}
