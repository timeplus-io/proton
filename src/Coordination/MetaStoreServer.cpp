#include <Coordination/MetaStoreServer.h>

#if !defined(ARCADIA_BUILD)
#   include "config_core.h"
#endif

#include <Coordination/LoggerWrapper.h>
#include <Coordination/MetaStateMachine.h>
#include <Coordination/MetaStateManager.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <chrono>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <string>
#include <filesystem>
#include <Poco/Util/Application.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
}

namespace
{

#if USE_SSL
void setSSLParams(nuraft::asio_service::options & asio_opts)
{
    const Poco::Util::LayeredConfiguration & config = Poco::Util::Application::instance().config();
    String certificate_file_property = "openSSL.server.certificateFile";
    String private_key_file_property = "openSSL.server.privateKeyFile";
    String root_ca_file_property = "openSSL.server.caConfig";

    if (!config.has(certificate_file_property))
        throw Exception("Server certificate file is not set.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    if (!config.has(private_key_file_property))
        throw Exception("Server private key file is not set.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    asio_opts.enable_ssl_ = true;
    asio_opts.server_cert_file_ = config.getString(certificate_file_property);
    asio_opts.server_key_file_ = config.getString(private_key_file_property);

    if (config.has(root_ca_file_property))
        asio_opts.root_cert_file_ = config.getString(root_ca_file_property);

    if (config.getBool("openSSL.server.loadDefaultCAFile", false))
        asio_opts.load_default_ca_file_ = true;

    if (config.getString("openSSL.server.verificationMode", "none") == "none")
        asio_opts.skip_verification_ = true;
}
#endif

std::string getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_metastore)
{
    /// the most specialized path
    if (config.has("metastore_server.snapshot_storage_path"))
        return config.getString("metastore_server.snapshot_storage_path");

    if (config.has("metastore_server.storage_path"))
        return std::filesystem::path{config.getString("metastore_server.storage_path")} / "snapshots";

    if (standalone_metastore)
        return std::filesystem::path{config.getString("path", METASTORE_DEFAULT_PATH)} / "snapshots";
    else
        return std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/snapshots";
}

std::string getStoragePathFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_metastore)
{
    /// the most specialized path
    if (config.has("metastore_server.storage_path"))
        return config.getString("metastore_server.storage_path");

    if (standalone_metastore)
        return config.getString("path", METASTORE_DEFAULT_PATH);
    else
        return std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination";
}

nuraft::ptr<nuraft::buffer> getBufferFromKVRequest(const Coordination::KVRequestPtr & request)
{
    DB::WriteBufferFromNuraftBuffer buf;
    request->write(buf);
    return buf.getBuffer();
}

Coordination::KVResponsePtr parseKVResponse(nuraft::ptr<nuraft::buffer> data)
{
    using namespace Coordination;

    ReadBufferFromNuraftBuffer buffer(*data);

    KVResponsePtr response = KVResponse::read(buffer);
    return response;
}

}

MetaStoreServer::MetaStoreServer(
    int server_id_,
    const CoordinationSettingsPtr & coordination_settings_,
    const Poco::Util::AbstractConfiguration & config,
    MetaSnapshotsQueue & snapshots_queue_,
    bool standalone_metastore)
    : server_id(server_id_)
    , coordination_settings(coordination_settings_)
    , state_machine(nuraft::cs_new<MetaStateMachine>(
                        snapshots_queue_,
                        getSnapshotsPathFromConfig(config, standalone_metastore),
                        getStoragePathFromConfig(config, standalone_metastore),
                        coordination_settings))
    , state_manager(nuraft::cs_new<MetaStateManager>(server_id, "metastore_server", config, coordination_settings, standalone_metastore))
    , log(&Poco::Logger::get("MetaStoreServer"))
{
    if (coordination_settings->quorum_reads)
        LOG_WARNING(log, "Quorum reads enabled, MetaStoreServer will work slower.");
}

void MetaStoreServer::startup()
{
    state_machine->init();

    state_manager->loadLogStore(state_machine->last_commit_index() + 1, coordination_settings->reserved_log_items);

    bool single_server = state_manager->getTotalServers() == 1;

    nuraft::raft_params params;
    if (single_server)
    {
        /// Don't make sense in single server mode
        params.heart_beat_interval_ = 0;
        params.election_timeout_lower_bound_ = 0;
        params.election_timeout_upper_bound_ = 0;
    }
    else
    {
        params.heart_beat_interval_ = coordination_settings->heart_beat_interval_ms.totalMilliseconds();
        params.election_timeout_lower_bound_ = coordination_settings->election_timeout_lower_bound_ms.totalMilliseconds();
        params.election_timeout_upper_bound_ = coordination_settings->election_timeout_upper_bound_ms.totalMilliseconds();
    }

    params.reserved_log_items_ = coordination_settings->reserved_log_items;
    params.snapshot_distance_ = coordination_settings->snapshot_distance;
    params.stale_log_gap_ = coordination_settings->stale_log_gap;
    params.fresh_log_gap_ = coordination_settings->fresh_log_gap;
    params.client_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds();
    params.auto_forwarding_ = coordination_settings->auto_forwarding;
    params.auto_forwarding_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds() * 2;
    params.max_append_size_ = coordination_settings->max_requests_batch_size;

    params.return_method_ = nuraft::raft_params::blocking;

    nuraft::asio_service::options asio_opts{};
    if (state_manager->isSecure())
    {
#if USE_SSL
        setSSLParams(asio_opts);
#else
        throw Exception{"SSL support for NuRaft is disabled because ClickHouse was built without SSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }

    launchRaftServer(params, asio_opts);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");
}

void MetaStoreServer::launchRaftServer(
    const nuraft::raft_params & params,
    const nuraft::asio_service::options & asio_opts)
{
    nuraft::raft_server::init_options init_options;

    init_options.skip_initial_election_timeout_ = state_manager->shouldStartAsFollower();
    init_options.start_server_in_constructor_ = false;
    init_options.raft_callback_ = [this] (nuraft::cb_func::Type type, nuraft::cb_func::Param * param)
    {
        return callbackFunc(type, param);
    };

    nuraft::ptr<nuraft::logger> logger = nuraft::cs_new<LoggerWrapper>("RaftInstance", coordination_settings->raft_logs_level);
    asio_service = nuraft::cs_new<nuraft::asio_service>(asio_opts, logger);
    asio_listener = asio_service->create_rpc_listener(state_manager->getPort(), logger);

    if (!asio_listener)
        return;

    nuraft::ptr<nuraft::delayed_task_scheduler> scheduler = asio_service;
    nuraft::ptr<nuraft::rpc_client_factory> rpc_cli_factory = asio_service;

    nuraft::ptr<nuraft::state_mgr> casted_state_manager = state_manager;
    nuraft::ptr<nuraft::state_machine> casted_state_machine = state_machine;

    /// raft_server creates unique_ptr from it
    nuraft::context * ctx = new nuraft::context(
        casted_state_manager, casted_state_machine,
        asio_listener, logger, rpc_cli_factory, scheduler, params);

    raft_instance = nuraft::cs_new<nuraft::raft_server>(ctx, init_options);

    raft_instance->start_server(init_options.skip_initial_election_timeout_);
    asio_listener->listen(raft_instance);
}

void MetaStoreServer::shutdownRaftServer()
{
    size_t timeout = coordination_settings->shutdown_timeout.totalSeconds();

    if (!raft_instance)
    {
        LOG_INFO(log, "RAFT doesn't start, shutdown not required");
        return;
    }

    raft_instance->shutdown();
    raft_instance.reset();

    if (asio_listener)
    {
        asio_listener->stop();
        asio_listener->shutdown();
    }

    if (asio_service)
    {
        asio_service->stop();
        size_t count = 0;
        while (asio_service->get_active_workers() != 0 && count < timeout * 100)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            count++;
        }
    }

    if (asio_service->get_active_workers() != 0)
        LOG_WARNING(log, "Failed to shutdown RAFT server in {} seconds", timeout);
}


void MetaStoreServer::shutdown()
{
    state_machine->shutdownStorage();
    state_manager->flushLogStore();
    shutdownRaftServer();
}

String MetaStoreServer::localGetByKey(const String & key) const
{
    String value;
    state_machine->getByKey(key, &value);
    return value;
}

std::vector<String> MetaStoreServer::localMultiGetByKeys(const std::vector<String> & keys) const
{
    std::vector<String> values;
    state_machine->multiGetByKeys(keys, &values);
    return values;
}

Coordination::KVResponsePtr MetaStoreServer::putRequest(const Coordination::KVRequestPtr & request)
{
    const auto & entry = getBufferFromKVRequest(request);
    auto ret = raft_instance->append_entries({entry});
    if (ret->get_accepted() &&
        ret->get_result_code() == nuraft::cmd_result_code::OK &&
        ret->get())
        return parseKVResponse(ret->get());

    /// error response
    auto response = std::make_shared<Coordination::KVEmptyResponse>();
    response->msg = ret->get_result_str();
    return response;
}

bool MetaStoreServer::isLeader() const
{
    return raft_instance->is_leader();
}

bool MetaStoreServer::isLeaderAlive() const
{
    return raft_instance->is_leader_alive();
}

bool MetaStoreServer::isAutoForward() const
{
    return raft_instance->get_current_params().auto_forwarding_;
}

int MetaStoreServer::getLeaderID() const
{
    return raft_instance->get_leader();
}

MetaClusterConfig MetaStoreServer::getClusterConfig() const
{
    return state_manager->load_config();
}

nuraft::cb_func::ReturnCode MetaStoreServer::callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param)
{
    if (initialized_flag)
        return nuraft::cb_func::ReturnCode::Ok;

    size_t last_commited = state_machine->last_commit_index();
    size_t next_index = state_manager->getLogStore()->next_slot();
    bool commited_store = false;
    if (next_index < last_commited || next_index - last_commited <= 1)
        commited_store = true;

    auto set_initialized = [this] ()
    {
        std::unique_lock lock(initialized_mutex);
        initialized_flag = true;
        initialized_cv.notify_all();
    };

    switch (type)
    {
        case nuraft::cb_func::BecomeLeader:
        {
            /// We become leader and store is empty or we already committed it
            if (commited_store || initial_batch_committed)
                set_initialized();
            return nuraft::cb_func::ReturnCode::Ok;
        }
        case nuraft::cb_func::BecomeFollower:
        case nuraft::cb_func::GotAppendEntryReqFromLeader:
        {
            if (param->leaderId != -1)
            {
                auto leader_index = raft_instance->get_leader_committed_log_idx();
                auto our_index = raft_instance->get_committed_log_idx();
                /// This may happen when we start RAFT cluster from scratch.
                /// Node first became leader, and after that some other node became leader.
                /// BecameFresh for this node will not be called because it was already fresh
                /// when it was leader.
                if (leader_index < our_index + coordination_settings->fresh_log_gap)
                    set_initialized();
            }
            return nuraft::cb_func::ReturnCode::Ok;
        }
        case nuraft::cb_func::BecomeFresh:
        {
            set_initialized(); /// We are fresh follower, ready to serve requests.
            return nuraft::cb_func::ReturnCode::Ok;
        }
        case nuraft::cb_func::InitialBatchCommited:
        {
            if (param->myId == param->leaderId) /// We have committed our log store and we are leader, ready to serve requests.
                set_initialized();
            initial_batch_committed = true;
            return nuraft::cb_func::ReturnCode::Ok;
        }
        default: /// ignore other events
            return nuraft::cb_func::ReturnCode::Ok;
    }
}

void MetaStoreServer::waitInit()
{
    std::unique_lock lock(initialized_mutex);
    int64_t timeout = coordination_settings->startup_timeout.totalMilliseconds();
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag.load(); }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}

}
