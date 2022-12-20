#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

#if USE_NURAFT

#include <Coordination/CoordinationSettings.h>
#include <Coordination/MetaStoreServer.h>
#include <Common/logger_useful.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <functional>

namespace DB
{
class MetaStoreDispatcher
{
private:
    CoordinationSettingsPtr coordination_settings;

    std::atomic_flag shutdown_called;

    /// RAFT wrapper. Most important class.
    std::unique_ptr<IMetaStoreServer> server;

    /// Whether this node host raft server
    bool enable_raft = true;

    Poco::Logger * log;

    /// meta store server port
    Int64 tcp_port;
    Int64 http_port;

public:
    MetaStoreDispatcher();

    void initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_metastore);

    void shutdown();

    ~MetaStoreDispatcher();

    String localGetByKey(const String & key, const String & namespace_) const { return server->localGetByKey(key, namespace_); }

    std::vector<String> localMultiGetByKeys(const std::vector<String> & keys, const String & namespace_) const
    {
        return server->localMultiGetByKeys(keys, namespace_);
    }

    std::vector<std::pair<String, String>> localRangeGetByNamespace(const String & prefix_, const String & namespace_) const
    {
        return server->localRangeGetByNamespace(prefix_, namespace_);
    }

    Coordination::KVResponsePtr putRequest(const Coordination::KVRequestPtr & request, const String & namespace_)
    {
        return server->putRequest(request, namespace_);
    }

    /// Whether or not this instance has raft server running
    bool hasServer() const { return enable_raft; }

    bool isLeader() const { return enable_raft ? (server->as<MetaStoreServer>())->isLeader() : false; }

    bool hasLeader() const { return enable_raft ? (server->as<MetaStoreServer>())->isLeaderAlive() : false; }

    bool isSupportAutoForward() const
    {
        return enable_raft ? (server->as<MetaStoreServer>())->isAutoForward() : false;
    }

    int getLeaderID() const { return enable_raft ? (server->as<MetaStoreServer>())->getLeaderID() : -1; }

    String getLeaderHostname() const;

    Int64 getTcpPort() const { return tcp_port; }

    Int64 getHttpPort() const { return http_port; }

    CoordinationSettingsPtr getSettings() const { return coordination_settings; }
};

}

#endif
