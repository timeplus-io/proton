#pragma once

#include <Cluster/MetaStore/LocalMetaQueue.h>
#include <Cluster/MetaStore/MetaDB.h>
#include <Cluster/MetaStore/ProposalRegistry.h>

#include <Bootstrap/ServerDescriptor.h>
#include <Cluster/Common/Cluster.h>
#include <Cluster/Common/FetchIsolation.h>
#include <Cluster/Common/TimeWheel/SystemTimer.h>
#include <Cluster/LocalLog/Common/FetchResult.h>
#include <Cluster/Requests/AlterTaskRequest.h>
#include <Cluster/Requests/AlterTaskResponse.h>
#include <Cluster/Requests/ChangeLogLevelRequest.h>
#include <Cluster/Requests/ChangeLogLevelResponse.h>
#include <Cluster/Requests/CreateAccessEntityRequest.h>
#include <Cluster/Requests/CreateAccessEntityResponse.h>
#include <Cluster/Requests/CreateAlertRequest.h>
#include <Cluster/Requests/CreateAlertResponse.h>
#include <Cluster/Requests/CreateDatabaseRequest.h>
#include <Cluster/Requests/CreateDatabaseResponse.h>
#include <Cluster/Requests/CreateDiskRequest.h>
#include <Cluster/Requests/CreateDiskResponse.h>
#include <Cluster/Requests/CreateFormatSchemaRequest.h>
#include <Cluster/Requests/CreateFormatSchemaResponse.h>
#include <Cluster/Requests/CreateNamedCollectionRequest.h>
#include <Cluster/Requests/CreateNamedCollectionResponse.h>
#include <Cluster/Requests/CreateStoragePolicyRequest.h>
#include <Cluster/Requests/CreateStoragePolicyResponse.h>
#include <Cluster/Requests/CreateStreamRequest.h>
#include <Cluster/Requests/CreateStreamResponse.h>
#include <Cluster/Requests/CreateTaskRequest.h>
#include <Cluster/Requests/CreateTaskResponse.h>
#include <Cluster/Requests/CreateUserDefinedFunctionRequest.h>
#include <Cluster/Requests/CreateUserDefinedFunctionResponse.h>
#include <Cluster/Requests/DeleteAccessEntityRequest.h>
#include <Cluster/Requests/DeleteAccessEntityResponse.h>
#include <Cluster/Requests/DeleteAlertRequest.h>
#include <Cluster/Requests/DeleteAlertResponse.h>
#include <Cluster/Requests/DeleteDatabaseRequest.h>
#include <Cluster/Requests/DeleteDatabaseResponse.h>
#include <Cluster/Requests/DeleteDiskRequest.h>
#include <Cluster/Requests/DeleteDiskResponse.h>
#include <Cluster/Requests/DeleteFormatSchemaRequest.h>
#include <Cluster/Requests/DeleteFormatSchemaResponse.h>
#include <Cluster/Requests/DeleteNamedCollectionRequest.h>
#include <Cluster/Requests/DeleteNamedCollectionResponse.h>
#include <Cluster/Requests/DeleteStoragePolicyRequest.h>
#include <Cluster/Requests/DeleteStoragePolicyResponse.h>
#include <Cluster/Requests/DeleteStreamRequest.h>
#include <Cluster/Requests/DeleteStreamResponse.h>
#include <Cluster/Requests/DeleteTaskRequest.h>
#include <Cluster/Requests/DeleteTaskResponse.h>
#include <Cluster/Requests/DeleteUserDefinedFunctionRequest.h>
#include <Cluster/Requests/DeleteUserDefinedFunctionResponse.h>
#include <Cluster/Requests/DropFormatSchemaCacheRequest.h>
#include <Cluster/Requests/DropFormatSchemaCacheResponse.h>
#include <Cluster/Requests/GetAlertRequest.h>
#include <Cluster/Requests/GetAlertResponse.h>
#include <Cluster/Requests/GetDatabaseRequest.h>
#include <Cluster/Requests/GetDatabaseResponse.h>
#include <Cluster/Requests/GetFormatSchemaRequest.h>
#include <Cluster/Requests/GetFormatSchemaResponse.h>
#include <Cluster/Requests/GetNamedCollectionRequest.h>
#include <Cluster/Requests/GetNamedCollectionResponse.h>
#include <Cluster/Requests/GetStreamRequest.h>
#include <Cluster/Requests/GetStreamResponse.h>
#include <Cluster/Requests/GetTaskRequest.h>
#include <Cluster/Requests/GetTaskResponse.h>
#include <Cluster/Requests/GetUserDefinedFunctionRequest.h>
#include <Cluster/Requests/GetUserDefinedFunctionResponse.h>
#include <Cluster/Requests/ListAccessEntitiesRequest.h>
#include <Cluster/Requests/ListAccessEntitiesResponse.h>
#include <Cluster/Requests/ListAlertsRequest.h>
#include <Cluster/Requests/ListAlertsResponse.h>
#include <Cluster/Requests/ListDatabasesRequest.h>
#include <Cluster/Requests/ListDatabasesResponse.h>
#include <Cluster/Requests/ListDisksRequest.h>
#include <Cluster/Requests/ListDisksResponse.h>
#include <Cluster/Requests/ListFormatSchemasRequest.h>
#include <Cluster/Requests/ListFormatSchemasResponse.h>
#include <Cluster/Requests/ListNamedCollectionsRequest.h>
#include <Cluster/Requests/ListNamedCollectionsResponse.h>
#include <Cluster/Requests/ListStoragePoliciesRequest.h>
#include <Cluster/Requests/ListStoragePoliciesResponse.h>
#include <Cluster/Requests/ListStreamsRequest.h>
#include <Cluster/Requests/ListStreamsResponse.h>
#include <Cluster/Requests/ListTasksRequest.h>
#include <Cluster/Requests/ListTasksResponse.h>
#include <Cluster/Requests/ListUserDefinedFunctionsRequest.h>
#include <Cluster/Requests/ListUserDefinedFunctionsResponse.h>
#include <Cluster/Requests/PipPythonPackageRequest.h>
#include <Cluster/Requests/PipPythonPackageResponse.h>
#include <Cluster/Requests/RenameStreamRequest.h>
#include <Cluster/Requests/RenameStreamResponse.h>
#include <Cluster/Requests/UpdateAccessEntityRequest.h>
#include <Cluster/Requests/UpdateAccessEntityResponse.h>
#include <Cluster/Requests/UpdateStreamSchemaRequest.h>
#include <Cluster/Requests/UpdateStreamSchemaResponse.h>
#include <Cluster/Requests/UpdateStreamSettingsRequest.h>
#include <Cluster/Requests/UpdateStreamSettingsResponse.h>
#include <Cluster/StoreConfig.h>
#include <Common/BackgroundSchedulePool.h>
#include <Common/SharedMutex.h>


namespace cluster
{
class Cluster;
class Server;

struct ProposalData;
using ProposalDataPtr = std::shared_ptr<ProposalData>;

namespace nlog
{
class Log;
using LogPtr = std::shared_ptr<Log>;
}

namespace meta
{
class MetaStore
{
public:
    MetaStore(StoreConfigPtr config_, std::shared_ptr<MetaDB> meta_db_);

    ~MetaStore() noexcept;

    void startup();
    void shutdown();

    CreateDatabaseResponsePtr createDatabase(CreateDatabaseRequestPtr req);
    DeleteDatabaseResponsePtr deleteDatabase(DeleteDatabaseRequestPtr req);
    ListDatabasesResponsePtr listDatabases(ListDatabasesRequestPtr req) const;
    GetDatabaseResponsePtr getDatabase(GetDatabaseRequestPtr req) const;
    CreateStreamResponsePtr createStream(CreateStreamRequestPtr req);
    UpdateStreamSettingsResponsePtr updateStreamSettings(UpdateStreamSettingsRequestPtr req);
    UpdateStreamSchemaResponsePtr updateStreamSchema(UpdateStreamSchemaRequestPtr req);
    DeleteStreamResponsePtr deleteStream(DeleteStreamRequestPtr req);
    RenameStreamResponsePtr renameStream(RenameStreamRequestPtr req);
    ListStreamsResponsePtr listStreams(ListStreamsRequestPtr req) const;
    ListStreamsResponsePtr listStreams() const;
    GetStreamResponsePtr getStream(GetStreamRequestPtr req) const;

    CallResultV<protocol::StreamDescriptorPtrs>
    getStreamLocal(const std::string & ns, const std::string & stream, size_t versions_requested) const;

    /// `createUserDefinedFunction` creates a UDF according to the request and persists the metadata
    CreateUserDefinedFunctionResponsePtr createUserDefinedFunction(CreateUserDefinedFunctionRequestPtr req);

    /// `deleteUserDefinedFunction` delete a UDF according to the request and clears the metadata
    DeleteUserDefinedFunctionResponsePtr deleteUserDefinedFunction(DeleteUserDefinedFunctionRequestPtr req);

    /// Collect UDF metadata from metastore
    ListUserDefinedFunctionsResponsePtr listUserDefinedFunctions(ListUserDefinedFunctionsRequestPtr req) const;

    ListUserDefinedFunctionsResponsePtr listAllUserDefinedFunctions() const;

    GetUserDefinedFunctionResponsePtr getUserDefinedFunction(GetUserDefinedFunctionRequestPtr req) const;

    CallResultV<protocol::UserDefinedFunctionDescriptorPtr> getUserDefinedFunctionLocal(const std::string & name) const;
    /// Multi-version
    CallResultV<protocol::UserDefinedFunctionDescriptorPtrs>
    getUserDefinedFunctionLocal(const std::string & name, size_t versions_requested) const;

    /// Notified to update UDF cache udf_by_name
    void notifyUserDefinedFunctionCreated(const std::string & func_name);
    void notifyUserDefinedFunctionDeleted(const std::string & func_name);

    /// `createFormatSchema` creates a format schema according to the request and persists the metadata
    CreateFormatSchemaResponsePtr createFormatSchema(CreateFormatSchemaRequestPtr req);

    /// `deleteFormatSchema` deletes a format schema according to the request and clears the metadata
    DeleteFormatSchemaResponsePtr deleteFormatSchema(DeleteFormatSchemaRequestPtr req);

    ListFormatSchemasResponsePtr listFormatSchemas(ListFormatSchemasRequestPtr req);

    /// `dropFormatSchemaCache` cleans up the format schema cache
    DropFormatSchemaCacheResponsePtr dropFormatSchemaCache(DropFormatSchemaCacheRequestPtr req);

    /// `changeLogLevel` changes log level for all nodes in cluster
    ChangeLogLevelResponsePtr changeLogLevel(ChangeLogLevelRequestPtr req);

    /// `pythonPackageOperation` manages Python packages across all nodes in cluster
    PipPythonPackageResponsePtr pythonPackageOperation(PipPythonPackageRequestPtr req);

    /// Multi-version
    GetFormatSchemaResponsePtr getFormatSchema(GetFormatSchemaRequestPtr req) const;

    /// Access Entity
    CreateAccessEntityResponsePtr createAccessEntity(CreateAccessEntityRequestPtr req);
    DeleteAccessEntityResponsePtr deleteAccessEntity(DeleteAccessEntityRequestPtr req);
    UpdateAccessEntityResponsePtr updateAccessEntity(UpdateAccessEntityRequestPtr req);
    ListAccessEntitiesResponsePtr listAccessEntities(ListAccessEntitiesRequestPtr req) const;

    /// Disk
    CreateDiskResponsePtr createDisk(CreateDiskRequestPtr req);
    DeleteDiskResponsePtr deleteDisk(DeleteDiskRequestPtr req);
    ListDisksResponsePtr listDisks(ListDisksRequestPtr req);

    /// Storage Policy
    CreateStoragePolicyResponsePtr createStoragePolicy(CreateStoragePolicyRequestPtr req);
    DeleteStoragePolicyResponsePtr deleteStoragePolicy(DeleteStoragePolicyRequestPtr req);
    ListStoragePoliciesResponsePtr listStoragePolicies(ListStoragePoliciesRequestPtr req);

    /// Alert
    CreateAlertResponsePtr createAlert(CreateAlertRequestPtr req);
    DeleteAlertResponsePtr deleteAlert(DeleteAlertRequestPtr req);
    ListAlertsResponsePtr listAlerts(ListAlertsRequestPtr req) const;
    GetAlertResponsePtr getAlert(GetAlertRequestPtr req) const;

    /// Task
    CreateTaskResponsePtr createTask(CreateTaskRequestPtr req);
    DeleteTaskResponsePtr deleteTask(DeleteTaskRequestPtr req);
    AlterTaskResponsePtr alterTask(AlterTaskRequestPtr req);
    ListTasksResponsePtr listTasks(ListTasksRequestPtr req) const;
    GetTaskResponsePtr getTask(GetTaskRequestPtr req) const;

    /// Named Collection
    CreateNamedCollectionResponsePtr createNamedCollection(CreateNamedCollectionRequestPtr req);
    DeleteNamedCollectionResponsePtr deleteNamedCollection(DeleteNamedCollectionRequestPtr req);
    ListNamedCollectionsResponsePtr listNamedCollections(ListNamedCollectionsRequestPtr req) const;
    GetNamedCollectionResponsePtr getNamedCollection(GetNamedCollectionRequestPtr req) const;

    bool isLocalNode(const std::string & host, uint16_t tcp_port, const std::string & fqdn_name) const;


    const StoreConfig & getConfig() const noexcept { return *config; }
    const Cluster & getCluster() const noexcept { return cluster; }

    void promoteSelfAssignID() { /* No-op */ }

    void setNodeID(NodeID node_id) noexcept { node_identity = node_id; }

    const std::string & clusterID() const noexcept;
    DB::ServerDescriptorPtr getServerDescriptor() const { return server_descriptor; }
    void setServerDescriptor(DB::ServerDescriptorPtr desc) { server_descriptor = desc; }

    CallResult<nlog::FetchResult> fetch(int64_t sn, uint64_t max_size, int64_t max_wait_ms) const;

    void setReady() noexcept { ready = true; }
    bool isReady() const noexcept { return ready.load(std::memory_order_relaxed); }
    bool isStopped() const noexcept { return stopped.load(std::memory_order_relaxed); }

    /// Always returns 1 for quorum size
    size_t quorumSize() const noexcept { return 1; }

    MetaDB & getMetaDB() noexcept { return *meta_db; }


    void ackProposal(uint64_t proposal_id, int64_t sn, int error_code, std::string && error_message);

    /// \param callback its life cycle and its reference's objects shall be longer than MetaStore
    /// registered the callbacks during bootstrap. The callbacks are called in a different thread usually.
    /// So make sure it's multiple thread safety. callback must be quick and shall never throw exception
    void registerRecoverCallback(std::function<void()> callback) { registered_recover_callbacks.push_back(std::move(callback)); }

    void onRecovery() const
    {
        for (const auto & callback : registered_recover_callbacks)
            callback();
    }

public:
    NodeID nodeID() const noexcept { return node_identity; }

    /// Internal API
    Error proposeMetaRequest(cluster::RequestPtr req, int64_t timeout_ms);


    CallResultV<AppliedSequence> appliedSequence(const StreamIDShard & stream_shard) const;

    CallResultV<AppliedSequence> lastAppliedSequence() const { return meta_db->lastAppliedSequence(); }

    const StreamShard & getMetaStreamShard() const noexcept { return STREAM_SHARD; }

private:
    const auto & storeDir() const noexcept { return config->data_dirs.front(); }

    ListStreamsResponsePtr listStreams(const std::string & ns) const;

    void init();
    void loopTimer();

    void writeCleanShutdownFile() const;
    bool removeCleanShutdownFile() const;

    template <typename MetaRequest, typename MetaResponse, bool resp_desc>
    std::shared_ptr<MetaResponse> processMetaRequestSync(std::shared_ptr<MetaRequest> req);

    /// Load UDFs from MetaDB to in-memory cache. Only called in constructor.
    void loadUserDefinedFunctions();

public:
    /// Direct setters for local components
    void setLocalMetaLog(std::shared_ptr<LocalMetaQueue> queue) { local_meta_queue = std::move(queue); }
    void setProposalRegistry(std::shared_ptr<ProposalRegistry> registry) { proposal_registry = std::move(registry); }

    std::shared_ptr<LocalMetaQueue> getLocalMetaQueue() const { return local_meta_queue; }
    std::shared_ptr<ProposalRegistry> getProposalRegistry() const { return proposal_registry; }

    uint64_t nextCorrelationID() noexcept
    {
        static std::atomic<uint64_t> correlation_id_counter{1};
        return correlation_id_counter.fetch_add(1);
    }

    static const StreamShard STREAM_SHARD;
    static const std::shared_ptr<StreamIDShard> STREAM_SHARD_PTR;

private:
    friend Server;

    StoreConfigPtr config;

    /// State flags (initialized early, used throughout)
    std::atomic_flag started;
    std::atomic<bool> stopped{false};

    /// Thread synchronization
    struct Mutexes
    {
        std::mutex stream_mu;
        DB::SharedMutex udf_mu;
        /// ...
    };
    mutable Mutexes mutexes;

    std::shared_ptr<MetaDB> meta_db;
    DB::ServerDescriptorPtr server_descriptor;
    NodeID node_identity{1}; /// Always node_id=1
    Cluster cluster;

    std::atomic<bool> ready = false;

    std::vector<std::function<void()>> registered_recover_callbacks;
    std::unordered_set<std::string> udf_names;

    /// Local components for metadata management
    std::shared_ptr<LocalMetaQueue> local_meta_queue;
    std::shared_ptr<ProposalRegistry> proposal_registry;

    std::optional<SystemTimer> timer;
    std::optional<ThreadPool> timer_looper;

    LoggerPtr logger;
};

}
}
