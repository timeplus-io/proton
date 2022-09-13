#pragma once

#include "IMetaStoreServer.h"
#include "KVRequest.h"
#include "KVResponse.h"

#include <Poco/Util/Application.h>

#include <base/logger_useful.h>

#include <vector>

namespace DB
{
class MetaStoreConnection : public IMetaStoreServer
{
private:
    /// health raft servers
    std::vector<Poco::URI> active_servers;

    /// unhealthy raft servers
    std::vector<Poco::URI> inactive_servers;

    Poco::Logger * log;

    int64_t startup_timeout;

public:
    MetaStoreConnection(const Poco::Util::AbstractConfiguration & config);

    ~MetaStoreConnection() override = default;

    String localGetByKey(const String & key, const String & namespace_) const override;

    std::vector<String> localMultiGetByKeys(const std::vector<String> & keys, const String & namespace_) const override;

    std::vector<std::pair<String, String>> localRangeGetByNamespace(const String & prefix_, const String & namespace_) const override;

    Coordination::KVResponsePtr putRequest(const Coordination::KVRequestPtr & request, const String & namespace_) override;

    void waitInit() override;

private:
    /// Forward REST req to MetaStoreHandler in raft servers
    std::pair<String, Int32> forwardRequest(
        const String & method,
        const String & query_id,
        const String & user,
        const String & password,
        const Poco::JSON::Object::Ptr & payload,
        const String & uri_parameter = {}) const;
};

}
