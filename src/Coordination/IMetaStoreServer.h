#pragma once

#include <Common/TypePromotion.h>
#include "KVRequest.h"
#include "KVResponse.h"

namespace DB
{
class IMetaStoreServer : public TypePromotion<IMetaStoreServer>
{
public:
    virtual ~IMetaStoreServer() = default;

    virtual String localGetByKey(const String & key, const String & namespace_) const = 0;

    virtual std::vector<String> localMultiGetByKeys(const std::vector<String> & keys, const String & namespace_) const = 0;

    virtual std::vector<std::pair<String, String>> localRangeGetByNamespace(const String & prefix_, const String & namespace_) const = 0;

    virtual Coordination::KVResponsePtr putRequest(const Coordination::KVRequestPtr & request, const String & namespace_) = 0;

    virtual void startup() {}

    virtual void waitInit() {}

    virtual void shutdown() {}
};

}
