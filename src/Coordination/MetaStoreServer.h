#pragma once

#include "CoordinationSettings.h"
#include "InMemoryLogStore.h"
#include "KVNamespaceAndPrefixHelper.h"
#include "KVRequest.h"
#include "KVResponse.h"
#include "MetaStateMachine.h"
#include "MetaStateManager.h"

#include <base/logger_useful.h>

#include <libnuraft/nuraft.hxx>

#include <unordered_map>

namespace DB
{
using MetaClusterConfig = nuraft::ptr<nuraft::cluster_config>;

class MetaStoreServer
{
private:
    const int server_id;

    CoordinationSettingsPtr coordination_settings;

    nuraft::ptr<MetaStateMachine> state_machine;

    nuraft::ptr<MetaStateManager> state_manager;

    nuraft::ptr<nuraft::raft_server> raft_instance;
    nuraft::ptr<nuraft::asio_service> asio_service;
    nuraft::ptr<nuraft::rpc_listener> asio_listener;

    std::mutex append_entries_mutex;

    std::mutex initialized_mutex;
    std::atomic<bool> initialized_flag = false;
    std::condition_variable initialized_cv;
    std::atomic<bool> initial_batch_committed = false;

    Poco::Logger * log;

    std::unordered_set<std::string> namespace_whitelist;

    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

    /// Almost copy-paste from nuraft::launcher, but with separated server init and start
    /// Allows to avoid race conditions.
    void launchRaftServer(const nuraft::raft_params & params, const nuraft::asio_service::options & asio_opts);

    void shutdownRaftServer();


public:
    MetaStoreServer(
        int server_id_,
        const CoordinationSettingsPtr & coordination_settings_,
        const Poco::Util::AbstractConfiguration & config,
        MetaSnapshotsQueue & snapshots_queue_,
        bool standalone_metastore);

    void startup();

    String localGetByKey(const String & key, const String & namespace_) const;

    std::vector<String> localMultiGetByKeys(const std::vector<String> & keys, const String & namespace_) const;

    std::vector<std::pair<String, String>> localRangeGetByNamespace(const String & prefix_, const String & namespace_) const;

    Coordination::KVResponsePtr putRequest(Coordination::KVRequestPtr request, const String & namespace_);

    bool isLeader() const;

    bool isLeaderAlive() const;

    bool isAutoForward() const;

    void waitInit();

    void shutdown();

    int getServerID() const { return server_id; }

    int getLeaderID() const;

    MetaClusterConfig getClusterConfig() const;

    const String & checkNamespace(const String & namespace_) const;
};

}
