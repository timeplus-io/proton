#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

#if USE_NURAFT

#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <functional>
#include <Coordination/MetaStoreServer.h>
#include <Coordination/CoordinationSettings.h>


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

    String localGetByKey(const String & key) const
    {
        return server->localGetByKey(key);
    }

    std::vector<String> localMultiGetByKeys(const std::vector<String> & keys) const
    {
        return server->localMultiGetByKeys(keys);
    }

    Coordination::KVResponsePtr putRequest(const Coordination::KVRequestPtr & request)
    {
        return server->putRequest(request);
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
