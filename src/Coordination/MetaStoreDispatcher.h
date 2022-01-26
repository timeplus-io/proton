#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

#if USE_NURAFT

#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Exception.h>
#include <Coordination/MetaStoreServer.h>
#include <Coordination/CoordinationSettings.h>
#include <base/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <functional>

namespace DB
{

class MetaStoreDispatcher
{

private:
    CoordinationSettingsPtr coordination_settings;

    /// Size depends on coordination settings
    MetaSnapshotsQueue snapshots_queue{1};

    std::atomic<bool> shutdown_called{false};

    /// RAFT wrapper. Most important class.
    std::unique_ptr<MetaStoreServer> server;

    Poco::Logger * log;

    /// meta store server port
    Int64 tcp_port;
    Int64 http_port;

public:
    MetaStoreDispatcher();

    void initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_metastore);

    void shutdown();

    ~MetaStoreDispatcher();

    String localGetByKey(const String & key, const String & namespace_) const
    {
        return server->localGetByKey(key, namespace_);
    }

    std::vector<String> localMultiGetByKeys(const std::vector<String> & keys, const String & namespace_) const
    {
        return server->localMultiGetByKeys(keys, namespace_);
    }

    std::vector<std::pair<String, String> > localRangeGetByNamespace(const String & prefix_, const String & namespace_) const
    {
        return server->localRangeGetByNamespace(prefix_, namespace_);
    }

    Coordination::KVResponsePtr putRequest(const Coordination::KVRequestPtr & request, const String & namespace_)
    {
        return server->putRequest(request, namespace_);
    }

    bool isLeader() const
    {
        return server->isLeader();
    }

    bool hasLeader() const
    {
        return server->isLeaderAlive();
    }

    bool isSupportAutoForward() const
    {
        return server->isAutoForward();
    }

    int getLeaderID() const
    {
        return server->getLeaderID();
    }

    String getLeaderHostname() const;

    Int64 getTcpPort() const
    {
        return tcp_port;
    }

    Int64 getHttpPort() const
    {
        return http_port;
    }

    CoordinationSettingsPtr getSettings() const
    {
        return coordination_settings;
    }
};

}

#endif
