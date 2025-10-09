#include <Cluster/LocalLog/Log/Log.h>
#include <Cluster/MetaStore/LocalMetaQueue.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/MetaStore/ProposalRegistry.h>

#include <base/ClockUtils.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>

#include <fcntl.h>


namespace DB::ErrorCodes
{
extern const int INVALID_DATA;
extern const int TIMEOUT_EXCEEDED;
extern const int RESOURCE_NOT_INITED;
extern const int RESOURCE_NOT_FOUND;
extern const int METADATA_VERSION_CHANGED;
extern const int UNKNOWN_FUNCTION;
}

namespace cluster::meta
{
namespace
{
const std::string CLEAN_SHUTDOWN_FILE = ".nlog_meta_clean_shutdown";
}

const StreamShard MetaStore::STREAM_SHARD{"__tp_sys", "__tp_mlog", StreamID{UInt128(1)}, /*shard_=*/0};
const std::shared_ptr<StreamIDShard> MetaStore::STREAM_SHARD_PTR = std::make_shared<StreamIDShard>(MetaStore::STREAM_SHARD.id_shard);

const std::string & MetaStore::clusterID() const noexcept
{
    static const std::string default_cluster_id = "cluster";
    return server_descriptor ? server_descriptor->cluster_id : default_cluster_id;
}

CreateDatabaseResponsePtr MetaStore::createDatabase(CreateDatabaseRequestPtr req)
{
    return processMetaRequestSync<CreateDatabaseRequest, CreateDatabaseResponse, /*resp_desc=*/false>(std::move(req));
}

DeleteDatabaseResponsePtr MetaStore::deleteDatabase(DeleteDatabaseRequestPtr req)
{
    return processMetaRequestSync<DeleteDatabaseRequest, DeleteDatabaseResponse, /*resp_desc=*/false>(std::move(req));
}

ListDatabasesResponsePtr MetaStore::listDatabases(ListDatabasesRequestPtr /*req*/) const
{
    auto result = meta_db->listDatabases();
    if (!result.hasError())
        return std::make_shared<ListDatabasesResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/1);
    else
        return std::make_shared<ListDatabasesResponse>(std::move(result.err), /*data_version_=*/1);
}

GetDatabaseResponsePtr MetaStore::getDatabase(GetDatabaseRequestPtr req) const
{
    const auto & req_data = req->data();
    auto result = meta_db->getDatabase(req_data.name, req_data.versions_requested);
    if (!result.hasError())
        return std::make_shared<GetDatabaseResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/1);
    else
        return std::make_shared<GetDatabaseResponse>(std::move(result.err), /*data_version_=*/1);
}

CreateStreamResponsePtr MetaStore::createStream(CreateStreamRequestPtr req)
{
    chassert(req);

    /// When the code flow reaches here, all of the validation has been done and succeeded
    auto & request_data = req->data();

    request_data.desc.create_timestamp_ms = DB::UTCMilliseconds::now();
    request_data.desc.last_modify_timestamp_ms = request_data.desc.create_timestamp_ms;

    return processMetaRequestSync<CreateStreamRequest, CreateStreamResponse, /*resp_desc=*/true>(std::move(req));
}

UpdateStreamSettingsResponsePtr MetaStore::updateStreamSettings(UpdateStreamSettingsRequestPtr req)
{
    chassert(req);

    const auto & req_data = req->data();
    chassert(!req_data.stream.name.empty());

    std::scoped_lock lock(mutexes.stream_mu);

    /// First get the stream definition
    auto result = meta_db->getStream(req_data.stream.ns, req_data.stream.name);
    if (result.hasError())
        return std::make_shared<UpdateStreamSettingsResponse>(std::move(result.err), /*data_version_=*/req->version());

    return processMetaRequestSync<UpdateStreamSettingsRequest, UpdateStreamSettingsResponse, /*resp_desc=*/false>(std::move(req));
}

UpdateStreamSchemaResponsePtr MetaStore::updateStreamSchema(UpdateStreamSchemaRequestPtr req)
{
    chassert(req);

    const auto & req_data = req->data();
    chassert(!req_data.stream.name.empty());

    std::scoped_lock lock(mutexes.stream_mu);

    /// First get the stream definition
    auto result = meta_db->getStream(req_data.stream.ns, req_data.stream.name);
    if (result.hasError())
        return std::make_shared<UpdateStreamSchemaResponse>(std::move(result.err), /*data_version_=*/req->version());

    return processMetaRequestSync<UpdateStreamSchemaRequest, UpdateStreamSchemaResponse, /*resp_desc=*/false>(std::move(req));
}

DeleteStreamResponsePtr MetaStore::deleteStream(DeleteStreamRequestPtr req)
{
    return processMetaRequestSync<DeleteStreamRequest, DeleteStreamResponse, /*resp_desc=*/false>(std::move(req));
}

RenameStreamResponsePtr MetaStore::renameStream(RenameStreamRequestPtr req)
{
    chassert(req);

    const auto & req_data = req->data();
    chassert(!req_data.new_stream.empty());

    if (req_data.new_stream == req_data.stream)
        return std::make_shared<RenameStreamResponse>(
            DB::ErrorCodes::INVALID_DATA, std::string("Can't rename stream to its existing name"), /*data_version_=*/req->version());

    std::scoped_lock lock(mutexes.stream_mu);

    /// Get the existing stream description
    auto result = meta_db->getStream(req_data.ns, req_data.stream);
    if (result.hasError())
        return std::make_shared<RenameStreamResponse>(std::move(result.err), /*data_version_=*/req->version());

    /// Change stream name in memory
    const auto & desc = result.result;
    desc->stream.name = req_data.new_stream;
    desc->last_modify_timestamp_ms = DB::UTCMilliseconds::now();
    desc->last_modified_by = req_data.modified_by;

    return processMetaRequestSync<RenameStreamRequest, RenameStreamResponse, /*resp_desc=*/false>(std::move(req));
}

GetStreamResponsePtr MetaStore::getStream(GetStreamRequestPtr req) const
{
    const auto & req_data = req->data();
    auto result = meta_db->getStream(req_data.ns, req_data.stream, req_data.versions_requested);
    if (!result.hasError())
        return std::make_shared<GetStreamResponse>(std::move(result.result), /*replica_leader_=*/1, /*sn_=*/0, /*data_version_=*/1);
    else
        return std::make_shared<GetStreamResponse>(std::move(result.err), /*data_version_=*/1);
}

CallResultV<protocol::StreamDescriptorPtrs>
MetaStore::getStreamLocal(const std::string & ns, const std::string & stream, size_t versions_requested) const
{
    return meta_db->getStream(ns, stream, versions_requested);
}

ListStreamsResponsePtr MetaStore::listStreams() const
{
    auto result = meta_db->listStreams();
    if (result.hasError())
        return std::make_shared<ListStreamsResponse>(std::move(result.err), /*data_version_=*/2);
    else
        return std::make_shared<ListStreamsResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/2);
}

ListStreamsResponsePtr MetaStore::listStreams(const std::string & ns) const
{
    auto result = meta_db->listStreams(ns);
    if (result.hasError())
        return std::make_shared<ListStreamsResponse>(std::move(result.err), /*data_version_=*/2);
    else
        return std::make_shared<ListStreamsResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/2);
}

ListStreamsResponsePtr MetaStore::listStreams(ListStreamsRequestPtr req) const
{
    const auto & req_data = req->data();
    if (!req_data.ns.empty() && !req_data.stream.empty())
    {
        auto result = meta_db->getStream(req_data.ns, req_data.stream);
        if (result.hasError())
            return std::make_shared<ListStreamsResponse>(std::move(result.err), /*data_version_=*/req->version());

        return std::make_shared<ListStreamsResponse>(std::vector{std::move(result.result)}, 1, /*sn_=*/0, /*data_version_=*/req->version());
    }

    if (req_data.ns.empty())
        return listStreams();

    return listStreams(req_data.ns);
}


CreateUserDefinedFunctionResponsePtr MetaStore::createUserDefinedFunction(CreateUserDefinedFunctionRequestPtr req)
{
    return processMetaRequestSync<CreateUserDefinedFunctionRequest, CreateUserDefinedFunctionResponse, /*resp_desc=*/true>(std::move(req));
}

DeleteUserDefinedFunctionResponsePtr MetaStore::deleteUserDefinedFunction(DeleteUserDefinedFunctionRequestPtr req)
{
    return processMetaRequestSync<DeleteUserDefinedFunctionRequest, DeleteUserDefinedFunctionResponse, /*resp_desc=*/false>(std::move(req));
}

ListUserDefinedFunctionsResponsePtr MetaStore::listUserDefinedFunctions(ListUserDefinedFunctionsRequestPtr req) const
{
    const auto & req_data = req->data();
    if (req_data.func_name.empty())
        return listAllUserDefinedFunctions();

    {
        std::shared_lock rlock(mutexes.udf_mu);
        if (!udf_names.contains(req_data.func_name))
            return std::make_shared<ListUserDefinedFunctionsResponse>(
                DB::ErrorCodes::UNKNOWN_FUNCTION, std::string{"UDF is not found"}, /*data_version_=*/req->version());
    }

    auto result = meta_db->getUserDefinedFunction(req_data.func_name);
    if (!result.hasError())
        return std::make_shared<ListUserDefinedFunctionsResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());
    else
        return std::make_shared<ListUserDefinedFunctionsResponse>(std::move(result.err), /*data_version_=*/req->version());
}

ListUserDefinedFunctionsResponsePtr MetaStore::listAllUserDefinedFunctions() const
{
    auto result = meta_db->listUserDefinedFunctions();
    if (!result.hasError())
        return std::make_shared<ListUserDefinedFunctionsResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/2);
    else
        return std::make_shared<ListUserDefinedFunctionsResponse>(std::move(result.err), /*data_version_=*/2);
}

GetUserDefinedFunctionResponsePtr MetaStore::getUserDefinedFunction(GetUserDefinedFunctionRequestPtr req) const
{
    const auto & req_data = req->data();
    auto result = meta_db->getUserDefinedFunction(req_data.name, req_data.versions_requested);
    if (!result.hasError())
        return std::make_shared<GetUserDefinedFunctionResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());
    else
        return std::make_shared<GetUserDefinedFunctionResponse>(std::move(result.err), /*data_version_=*/req->version());
}

CallResultV<protocol::UserDefinedFunctionDescriptorPtr> MetaStore::getUserDefinedFunctionLocal(const std::string & name) const
{
    return meta_db->getUserDefinedFunction(name);
}

CallResultV<protocol::UserDefinedFunctionDescriptorPtrs>
MetaStore::getUserDefinedFunctionLocal(const std::string & name, size_t versions_requested) const
{
    return meta_db->getUserDefinedFunction(name, versions_requested);
}

void MetaStore::notifyUserDefinedFunctionCreated(const std::string & func_name)
{
    std::unique_lock wlock(mutexes.udf_mu);
    udf_names.insert(func_name);
}

void MetaStore::notifyUserDefinedFunctionDeleted(const std::string & func_name)
{
    std::unique_lock wlock(mutexes.udf_mu);
    udf_names.erase(func_name);
}

void MetaStore::loadUserDefinedFunctions()
{
    chassert(meta_db);
    auto result = meta_db->listUserDefinedFunctions();
    if (result.hasError())
    {
        LOG_ERROR(logger, "Failed to load UDFs from MetaDB: {}", result.errorString());
        return;
    }

    for (auto & udf : result.result)
        udf_names.insert(udf->name);
}

CreateFormatSchemaResponsePtr MetaStore::createFormatSchema(CreateFormatSchemaRequestPtr req)
{
    return processMetaRequestSync<CreateFormatSchemaRequest, CreateFormatSchemaResponse, /*resp_desc=*/true>(std::move(req));
}

DeleteFormatSchemaResponsePtr MetaStore::deleteFormatSchema(DeleteFormatSchemaRequestPtr req)
{
    return processMetaRequestSync<DeleteFormatSchemaRequest, DeleteFormatSchemaResponse, /*resp_desc=*/false>(std::move(req));
}

ListFormatSchemasResponsePtr MetaStore::listFormatSchemas(ListFormatSchemasRequestPtr req)
{
    const auto & req_data = req->data();
    if (!req_data.schema_name.empty() && !req_data.format.empty())
    {
        auto result = meta_db->getFormatSchema(req_data.schema_name, req_data.format);
        if (!result.hasError())
            return std::make_shared<ListFormatSchemasResponse>(
                std::vector{std::move(result.result)}, 1, /*sn_=*/0, /*data_version_=*/req->version());

        return std::make_shared<ListFormatSchemasResponse>(std::move(result.err), /*data_version_=*/req->version());
    }
    else
    {
        auto result = meta_db->listFormatSchemas();
        if (!result.hasError())
            return std::make_shared<ListFormatSchemasResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());

        return std::make_shared<ListFormatSchemasResponse>(std::move(result.err), /*data_version_=*/req->version());
    }
}

GetFormatSchemaResponsePtr MetaStore::getFormatSchema(GetFormatSchemaRequestPtr req) const
{
    const auto & req_data = req->data();
    auto result = meta_db->getFormatSchema(req_data.schema_name, req_data.format, req_data.versions_requested);
    if (!result.hasError())
        return std::make_shared<GetFormatSchemaResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());
    else
        return std::make_shared<GetFormatSchemaResponse>(std::move(result.err), /*data_version_=*/req->version());
}

DropFormatSchemaCacheResponsePtr MetaStore::dropFormatSchemaCache(DropFormatSchemaCacheRequestPtr req)
{
    return processMetaRequestSync<DropFormatSchemaCacheRequest, DropFormatSchemaCacheResponse, /*resp_desc=*/false>(std::move(req));
}

ChangeLogLevelResponsePtr MetaStore::changeLogLevel(ChangeLogLevelRequestPtr req)
{
    return processMetaRequestSync<ChangeLogLevelRequest, ChangeLogLevelResponse, /*resp_desc=*/false>(std::move(req));
}

PipPythonPackageResponsePtr MetaStore::pythonPackageOperation(PipPythonPackageRequestPtr req)
{
    return processMetaRequestSync<PipPythonPackageRequest, PipPythonPackageResponse, /*resp_desc=*/false>(std::move(req));
}

CreateAccessEntityResponsePtr MetaStore::createAccessEntity(CreateAccessEntityRequestPtr req)
{
    return processMetaRequestSync<CreateAccessEntityRequest, CreateAccessEntityResponse, /*resp_desc=*/false>(std::move(req));
}

DeleteAccessEntityResponsePtr MetaStore::deleteAccessEntity(DeleteAccessEntityRequestPtr req)
{
    return processMetaRequestSync<DeleteAccessEntityRequest, DeleteAccessEntityResponse, /*resp_desc=*/false>(std::move(req));
}

UpdateAccessEntityResponsePtr MetaStore::updateAccessEntity(UpdateAccessEntityRequestPtr req)
{
    return processMetaRequestSync<UpdateAccessEntityRequest, UpdateAccessEntityResponse, /*resp_desc=*/false>(std::move(req));
}

ListAccessEntitiesResponsePtr MetaStore::listAccessEntities(ListAccessEntitiesRequestPtr req) const
{
    auto result = meta_db->listAccessEntities();
    if (!result.hasError())
        return std::make_shared<ListAccessEntitiesResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());
    else
        return std::make_shared<ListAccessEntitiesResponse>(std::move(result.err), /*data_version_=*/req->version());
}

CreateDiskResponsePtr MetaStore::createDisk(CreateDiskRequestPtr req)
{
    return processMetaRequestSync<CreateDiskRequest, CreateDiskResponse, /*resp_desc=*/false>(std::move(req));
}

DeleteDiskResponsePtr MetaStore::deleteDisk(DeleteDiskRequestPtr req)
{
    return processMetaRequestSync<DeleteDiskRequest, DeleteDiskResponse, /*resp_desc=*/false>(std::move(req));
}

ListDisksResponsePtr MetaStore::listDisks(ListDisksRequestPtr req)
{
    auto result = meta_db->listDisks(req->data().name);
    if (!result.hasError())
        return std::make_shared<ListDisksResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());
    else
        return std::make_shared<ListDisksResponse>(std::move(result.err), /*data_version_=*/req->version());
}

CreateStoragePolicyResponsePtr MetaStore::createStoragePolicy(CreateStoragePolicyRequestPtr req)
{
    return processMetaRequestSync<CreateStoragePolicyRequest, CreateStoragePolicyResponse, /*resp_desc=*/false>(std::move(req));
}

DeleteStoragePolicyResponsePtr MetaStore::deleteStoragePolicy(DeleteStoragePolicyRequestPtr req)
{
    return processMetaRequestSync<DeleteStoragePolicyRequest, DeleteStoragePolicyResponse, /*resp_desc=*/false>(std::move(req));
}

ListStoragePoliciesResponsePtr MetaStore::listStoragePolicies(ListStoragePoliciesRequestPtr req)
{
    auto result = meta_db->listStoragePolicies(req->data().name);
    if (!result.hasError())
        return std::make_shared<ListStoragePoliciesResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());
    else
        return std::make_shared<ListStoragePoliciesResponse>(std::move(result.err), /*data_version_=*/req->version());
}

CreateAlertResponsePtr MetaStore::createAlert(CreateAlertRequestPtr req)
{
    return processMetaRequestSync<CreateAlertRequest, CreateAlertResponse, /*resp_desc=*/true>(std::move(req));
}

DeleteAlertResponsePtr MetaStore::deleteAlert(DeleteAlertRequestPtr req)
{
    return processMetaRequestSync<DeleteAlertRequest, DeleteAlertResponse, /*resp_desc=*/false>(std::move(req));
}

ListAlertsResponsePtr MetaStore::listAlerts(ListAlertsRequestPtr req) const
{
    const auto & req_data = req->data();
    if (!req_data.ns.empty() && !req_data.name.empty())
    {
        auto result = meta_db->getAlert(req_data.ns, req_data.name);
        if (!result.hasError())
            return std::make_shared<ListAlertsResponse>(
                std::vector{std::move(result.result)}, 1, /*sn_=*/0, /*data_version_=*/req->version());

        return std::make_shared<ListAlertsResponse>(std::move(result.err), /*data_version_=*/req->version());
    }
    else if (!req_data.ns.empty())
    {
        auto result = meta_db->listAlerts(req_data.ns);
        if (!result.hasError())
            return std::make_shared<ListAlertsResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());

        return std::make_shared<ListAlertsResponse>(std::move(result.err), /*data_version_=*/req->version());
    }
    else
    {
        auto result = meta_db->listAlerts();
        if (!result.hasError())
            return std::make_shared<ListAlertsResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());

        return std::make_shared<ListAlertsResponse>(std::move(result.err), /*data_version_=*/req->version());
    }
}

GetAlertResponsePtr MetaStore::getAlert(GetAlertRequestPtr req) const
{
    const auto & req_data = req->data();
    auto result = meta_db->getAlert(req_data.ns, req_data.name, req_data.versions_requested);
    if (!result.hasError())
        return std::make_shared<GetAlertResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());
    else
        return std::make_shared<GetAlertResponse>(std::move(result.err), /*data_version_=*/req->version());
}

CreateTaskResponsePtr MetaStore::createTask(CreateTaskRequestPtr req)
{
    return processMetaRequestSync<CreateTaskRequest, CreateTaskResponse, /*resp_desc=*/false>(std::move(req));
}

DeleteTaskResponsePtr MetaStore::deleteTask(DeleteTaskRequestPtr req)
{
    return processMetaRequestSync<DeleteTaskRequest, DeleteTaskResponse, /*resp_desc=*/false>(std::move(req));
}

AlterTaskResponsePtr MetaStore::alterTask(AlterTaskRequestPtr req)
{
    return processMetaRequestSync<AlterTaskRequest, AlterTaskResponse, /*resp_desc=*/false>(std::move(req));
}

ListTasksResponsePtr MetaStore::listTasks(ListTasksRequestPtr req) const
{
    const auto & req_data = req->data();
    if (!req_data.ns.empty() && !req_data.name.empty())
    {
        auto result = meta_db->getTask(req_data.ns, req_data.name);
        if (!result.hasError())
            return std::make_shared<ListTasksResponse>(
                std::vector{std::move(result.result)}, 1, /*sn_=*/0, /*data_version_=*/req->version());

        return std::make_shared<ListTasksResponse>(std::move(result.err), /*data_version_=*/req->version());
    }
    else if (!req_data.ns.empty())
    {
        auto result = meta_db->listTasks(req_data.ns);
        if (!result.hasError())
            return std::make_shared<ListTasksResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());

        return std::make_shared<ListTasksResponse>(std::move(result.err), /*data_version_=*/req->version());
    }
    else
    {
        auto result = meta_db->listTasks();
        if (!result.hasError())
            return std::make_shared<ListTasksResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());

        return std::make_shared<ListTasksResponse>(std::move(result.err), /*data_version_=*/req->version());
    }
}

GetTaskResponsePtr MetaStore::getTask(GetTaskRequestPtr req) const
{
    const auto & req_data = req->data();
    auto result = meta_db->getTask(req_data.ns, req_data.name, req_data.versions_requested);
    if (!result.hasError())
        return std::make_shared<GetTaskResponse>(std::move(result.result), 1, /*sn_=*/0, /*data_version_=*/req->version());
    else
        return std::make_shared<GetTaskResponse>(std::move(result.err), /*data_version_=*/req->version());
}

Error MetaStore::proposeMetaRequest(cluster::RequestPtr req, int64_t timeout_ms)
{
    /// Always use local mode
    if (local_meta_queue && proposal_registry)
    {
        UNUSED(timeout_ms); // Not used in local mode

        auto correlation_id = nextCorrelationID();
        auto future = proposal_registry->registerProposal(correlation_id);

        /// Serialize the request
        cluster::RequestHeader header;
        header.data() = protocol::RequestHeaderData(req->opCode(), req->version(), correlation_id);

        auto serialized_bytes = req->serializeWithHeader(header);
        String serialized_data(reinterpret_cast<const char *>(serialized_bytes.data()), serialized_bytes.size());

        /// Append to LocalMetaQueue with correlation_id
        auto append_result = local_meta_queue->append(serialized_data, correlation_id);

        if (append_result.hasError())
        {
            proposal_registry->cancelProposal(correlation_id, Error(append_result.err.error_code, append_result.err.error_message));
            return Error{append_result.err.error_code, append_result.err.error_message};
        }

        /// Wait for completion
        auto error = future.get();
        return error;
    }

    /// Distributed mode disabled
    return Error{DB::ErrorCodes::NOT_IMPLEMENTED, "Distributed mode disabled"};
}

template <typename MetaRequest, typename MetaResponse, bool resp_desc>
std::shared_ptr<MetaResponse> MetaStore::processMetaRequestSync(std::shared_ptr<MetaRequest> req)
{
    chassert(req);

    auto timeout_ms = req->data().timeout_ms;
    auto req_version = req->version();

    LOG_DEBUG(logger, "propose request: req={{{}}} timeout_ms={}", req->data().string(), timeout_ms);

    /// Use LocalMetaQueue for persistence
    if (local_meta_queue && proposal_registry)
    {
        auto correlation_id = nextCorrelationID();

        /// Register proposal for tracking
        auto future = proposal_registry->registerProposal(correlation_id);

        /// Serialize the request
        cluster::RequestHeader header;
        header.data() = protocol::RequestHeaderData(req->opCode(), req->version(), correlation_id);

        auto serialized_bytes = req->serializeWithHeader(header);
        String serialized_data(reinterpret_cast<const char *>(serialized_bytes.data()), serialized_bytes.size());

        /// Append to LocalMetaQueue (persist first) - pass the serialized request and correlation_id
        auto append_result = local_meta_queue->append(serialized_data, correlation_id);
        if (append_result.hasError())
        {
            proposal_registry->cancelProposal(correlation_id, Error(append_result.err.error_code, append_result.err.error_message));
            return std::make_shared<MetaResponse>(
                append_result.err.error_code, append_result.err.error_message, /*data_version_=*/req_version);
        }

        /// Wait for proposal completion (will be fulfilled by MetadataUpdater)
        auto error = future.get();

        if (error.hasError())
        {
            return std::make_shared<MetaResponse>(std::move(error), /*data_version_=*/req_version);
        }

        /// Success response
        if constexpr (resp_desc)
        {
            return std::make_shared<MetaResponse>(
                std::move(req->data().desc),
                nodeID(), /// Leader is self
                append_result.result, /// Use sequence number from queue
                /*data_version_=*/req_version);
        }
        else
        {
            return std::make_shared<MetaResponse>(
                nodeID(), /// Leader is self
                append_result.result, /// Use sequence number from queue
                /*data_version_=*/req_version);
        }
    }

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "processMetaRequestSync called without local mode or missing components");
}


CallResult<nlog::FetchResult> MetaStore::fetch(int64_t sn, uint64_t max_size, int64_t max_wait_ms) const
{
    if (!local_meta_queue)
        return CallResult<nlog::FetchResult>(DB::ErrorCodes::NOT_IMPLEMENTED);

    auto result = local_meta_queue->fetch(sn, max_size, max_wait_ms);
    if (result.hasError())
        return CallResult<nlog::FetchResult>(result.err.error_code);

    if (result.result)
        return CallResult<nlog::FetchResult>(*result.result);
    else
        return CallResult<nlog::FetchResult>(DB::ErrorCodes::NOT_IMPLEMENTED);
}

void MetaStore::writeCleanShutdownFile() const
{
    auto clean_shutdown_file = storeDir() / CLEAN_SHUTDOWN_FILE;
    ::open(clean_shutdown_file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0666);
}

/// \return true if there is clean shutdown file and the file is cleaned and false if the file doesn't exist
bool MetaStore::removeCleanShutdownFile() const
{
    auto clean_shutdown_file = storeDir() / CLEAN_SHUTDOWN_FILE;
    if (fs::exists(clean_shutdown_file))
    {
        LOG_INFO(logger, "Skipping recovery for meta log in {} since clean shutdown file was found", storeDir().c_str());

        /// Delete the clean shutdown file, so that if the meta node crashes while loading the log,
        /// it is considered a hard shutdown during next boost up.
        std::error_code ec;
        fs::remove(clean_shutdown_file, ec);

        return true;
    }
    else
    {
        LOG_INFO(logger, "Attempting recovery for meta logs in {} since no clean shutdown file was found", storeDir().c_str());

        return false;
    }
}
}
