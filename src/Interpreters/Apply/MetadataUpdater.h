#pragma once

#include <Cluster/Apply/Task.h>
#include <Cluster/Protocol/AlertDescriptor.h>
#include <Cluster/Protocol/FormatSchemaDescriptor.h>
#include <Cluster/Protocol/InternalProtocol.h>
#include <Cluster/Protocol/StreamDescriptor.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>

#include <Databases/IDatabase.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Common/ThreadPool.h>

namespace cluster
{
namespace meta
{
class MetaStore;
}

struct CreateStreamRequest;
struct DeleteStreamRequest;
struct UpdateStreamSettingsRequest;
struct UpdateStreamSchemaRequest;
struct CreateUserDefinedFunctionRequest;
struct DeleteUserDefinedFunctionRequest;
struct CreateFormatSchemaRequest;
struct DeleteFormatSchemaRequest;
struct DropFormatSchemaCacheRequest;
struct ChangeLogLevelRequest;
struct PipPythonPackageRequest;
struct CreateAccessEntityRequest;
struct DeleteAccessEntityRequest;
struct UpdateAccessEntityRequest;
struct CreateDatabaseRequest;
struct DeleteDatabaseRequest;
struct CreateDiskRequest;
struct ListDisksRequest;
struct DeleteDiskRequest;
struct RenameStreamRequest;
struct CreateStoragePolicyRequest;
struct ListStoragePoliciesRequest;
struct DeleteStoragePolicyRequest;
struct AssignMaterializedViewRequest;
struct CreateAlertRequest;
struct DeleteAlertRequest;
class AlterTaskRequest;
struct CreateTaskRequest;
struct DeleteTaskRequest;
struct RequestHeader;

namespace protocol
{
struct CreateStreamRequestData;
struct DeleteStreamRequestData;
struct UpdateStreamSettingsRequestData;
struct UpdateStreamSchemaRequestData;
struct CreateUserDefinedFunctionRequestData;
struct DeleteUserDefinedFunctionRequestData;
struct RenameStreamRequestData;
}
}

namespace Poco
{
class Logger;
}

namespace DB
{

struct StorageInMemoryMetadata;
struct StorageID;

/// `MetadataUpdater` continuously tails meta log and replays / executes the DDL
/// queries defined in the meta log records if the current role not primary.
/// After successfully replays the DDL in the meta log, it also updates
/// the snapshot kv store.
class MetadataUpdater final : public cluster::apply::Task
{
public:
    explicit MetadataUpdater(const ContextMutablePtr & global_context_);

    ~MetadataUpdater() override;

    void startup() override;
    void shutdown() override;

    void onRecovery();

private:
    void apply(cluster::EntryPtrs entries) override;

    void backgroundPoll();

    static bool isPermanentError(int error_code);

    [[nodiscard]] int executeWithRetry(std::function<int()> func) const;

    void handleFailedRequest(uint64_t correlation_id, uint64_t sn, int err_code, std::string && err_msg) const;

    /// Clean up pending request after successful apply (mode)
    void cleanupPendingRequestIfNeeded(uint64_t sn) const;

    void handleCreateStream(const cluster::CreateStreamRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    [[nodiscard]] std::pair<StoragePtr, int32_t>
    doHandleCreateStream(const cluster::protocol::CreateStreamRequestData & request_data) const;

    void handleDeleteStream(const cluster::DeleteStreamRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    [[nodiscard]] int32_t doHandleDeleteStream(const cluster::protocol::DeleteStreamRequestData & request_data) const;

    cluster::CallResultV<cluster::protocol::StreamDescriptorPtr>
    loadStreamDescriptor(const cluster::Stream & stream, const cluster::RequestHeader & request_header, uint64_t sn) const;

    int updateStreamAndCommit(
        const cluster::RequestHeader & request_header,
        uint64_t sn,
        uint32_t version_before_update,
        cluster::protocol::StreamDescriptor & desc,
        std::function<int(cluster::protocol::StreamDescriptor &)> update) const;

    void handleUpdateStreamSettings(
        const cluster::UpdateStreamSettingsRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    [[nodiscard]] int doHandleUpdateStreamSettings(
        const cluster::protocol::UpdateStreamSettingsRequestData & request_data, cluster::protocol::StreamDescriptor & desc) const;

    void handleAlterStreamSchema(
        const cluster::UpdateStreamSchemaRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    [[nodiscard]] int doHandleAlterStreamSchema(
        const cluster::protocol::UpdateStreamSchemaRequestData & request_data, cluster::protocol::StreamDescriptor & desc) const;

    void handleRenameStream(const cluster::RenameStreamRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    [[nodiscard]] int doHandleRenameStream(
        const cluster::protocol::RenameStreamRequestData & request_data,
        cluster::protocol::StreamDescriptorPtr & desc,
        DatabasePtr db,
        StoragePtr table) const;

    void handleCreateUserDefinedFunction(
        const cluster::CreateUserDefinedFunctionRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleDeleteUserDefinedFunction(
        const cluster::DeleteUserDefinedFunctionRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleCreateFormatSchema(
        const cluster::CreateFormatSchemaRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleDeleteFormatSchema(
        const cluster::DeleteFormatSchemaRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    cluster::CallResultV<cluster::protocol::FormatSchemaDescriptorPtr>
    loadFormatSchemaDescriptor(const std::string & name, const std::string & format) const;

    void handleDropFormatSchemaCache(
        const cluster::DropFormatSchemaCacheRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void
    handleChangeLogLevel(const cluster::ChangeLogLevelRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handlePipPythonPackage(
        const cluster::PipPythonPackageRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleCreateAccessEntity(
        const cluster::CreateAccessEntityRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleDeleteAccessEntity(
        const cluster::DeleteAccessEntityRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleUpdateAccessEntity(
        const cluster::UpdateAccessEntityRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void
    handleCreateDatabase(const cluster::CreateDatabaseRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void
    handleDeleteDatabase(const cluster::DeleteDatabaseRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleCreateDisk(const cluster::CreateDiskRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleDeleteDisk(const cluster::DeleteDiskRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleCreateStoragePolicy(
        const cluster::CreateStoragePolicyRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleDeleteStoragePolicy(
        const cluster::DeleteStoragePolicyRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleAssignMaterializedView(
        cluster::AssignMaterializedViewRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleCreateAlert(const cluster::CreateAlertRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleDeleteAlert(const cluster::DeleteAlertRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    cluster::CallResultV<cluster::protocol::AlertDescriptorPtr> loadAlertDescriptor(const std::string & ns, const std::string & name) const;

    void handleCreateTask(const cluster::CreateTaskRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleDeleteTask(const cluster::DeleteTaskRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    void handleAlterTask(const cluster::AlterTaskRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const;

    cluster::CallResultV<cluster::protocol::TaskDescriptorPtr> loadTaskDescriptor(const std::string & ns, const std::string & name) const;

    void updateInMemoryMetadataForStream(
        StoragePtr table,
        const StorageID & table_id,
        const cluster::protocol::StreamDescriptor & desc,
        StorageInMemoryMetadata & new_metadata) const;

private:
    std::atomic_flag started;
    std::atomic_flag stopped;

    cluster::meta::MetaStore * meta_store = nullptr;

    ContextMutablePtr global_context;

    std::atomic<int64_t> next_sn = 0;

    ThreadPool poller;

    LoggerPtr logger;
};
}
