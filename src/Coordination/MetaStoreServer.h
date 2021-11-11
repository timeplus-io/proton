#pragma once

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <Coordination/InMemoryLogStore.h>
#include <Coordination/KVRequest.h>
#include <Coordination/KVResponse.h>
#include <Coordination/MetaStateManager.h>
#include <Coordination/MetaStateMachine.h>
#include <Coordination/CoordinationSettings.h>
#include <unordered_map>
#include <common/logger_useful.h>

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

    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

    /// Almost copy-paste from nuraft::launcher, but with separated server init and start
    /// Allows to avoid race conditions.
    void launchRaftServer(
        const nuraft::raft_params & params,
        const nuraft::asio_service::options & asio_opts);

    void shutdownRaftServer();


public:
    MetaStoreServer(
        int server_id_,
        const CoordinationSettingsPtr & coordination_settings_,
        const Poco::Util::AbstractConfiguration & config,
        MetaSnapshotsQueue & snapshots_queue_,
        bool standalone_metastore);

    void startup();

    String localGetByKey(const String & key) const;

    std::vector<String> localMultiGetByKeys(const std::vector<String> & keys) const;

    Coordination::KVResponsePtr putRequest(const Coordination::KVRequestPtr & request);

    bool isLeader() const;

    bool isLeaderAlive() const;

    bool isAutoForward() const;

    void waitInit();

    void shutdown();

    int getServerID() const { return server_id; }

    int getLeaderID() const;

    MetaClusterConfig getClusterConfig() const;
};

}
