#pragma once

#include <Cluster/Common/AppliedSequence.h>
#include <Cluster/Common/EpochSequence.h>
#include <Cluster/Common/MetaKeySpace.h>
#include <Cluster/Protocol/AccessEntityDescription.h>
#include <Cluster/Protocol/AlertDescriptor.h>
#include <Cluster/Protocol/DatabaseDescriptor.h>
#include <Cluster/Protocol/DiskDescriptor.h>
#include <Cluster/Protocol/FormatSchemaDescriptor.h>
#include <Cluster/Protocol/MaterializedViewAssignment.h>
#include <Cluster/Protocol/NamedCollectionDescriptor.h>
#include <Cluster/Protocol/StoragePolicyDescriptor.h>
#include <Cluster/Protocol/StreamDescriptor.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Common/logger_useful.h>

#include <filesystem>

namespace rocksdb
{
class DB;
class ColumnFamilyHandle;
class WriteBatch;
class Snapshot;
}

namespace cluster::meta
{
class MetaDB final
{
public:
    MetaDB(
        const std::filesystem::path & meta_dir, const StreamIDShard & meta_stream_shard_, size_t metadata_keep_versions, LoggerPtr logger_);

    ~MetaDB();

    /// Stream CRUD
    Error saveStream(const protocol::StreamDescriptor & desc, AppliedSequence applied_sn);

    Error deleteStream(const std::string & ns, const std::string & stream, AppliedSequence applied_sn);

    Error renameStream(const std::string & old_name, const protocol::StreamDescriptor & desc, AppliedSequence applied_sn);

    CallResultV<protocol::StreamDescriptorPtr> getStream(const std::string & ns, const std::string & stream) const;
    /// Versioned schema
    /// \param versions_requested indicates how many versions to return. If requested versions are bigger than the versions it has, all versions
    ///        will be returned; otherwise the latest versions of schemas will be returned
    /// \return a list of stream descriptors from oldest to latest version (the last element is the latest version)
    CallResultV<protocol::StreamDescriptorPtrs>
    getStream(const std::string & ns, const std::string & stream, size_t versions_requested) const;

    CallResultV<protocol::StreamDescriptorPtrs> listStreams(const std::string & ns) const;

    CallResultV<protocol::StreamDescriptorPtrs> listStreams(std::vector<std::string> * corrupted_keys = nullptr) const;

    /// UDF CRUD
    Error saveUserDefinedFunction(const protocol::UserDefinedFunctionDescriptor & desc, AppliedSequence applied_sn);

    Error deleteUserDefinedFunction(const std::string & name, AppliedSequence applied_sn);

    CallResultV<protocol::UserDefinedFunctionDescriptorPtr> getUserDefinedFunction(const std::string & name) const;

    CallResultV<protocol::UserDefinedFunctionDescriptorPtrs>
    getUserDefinedFunction(const std::string & name, size_t versions_requested) const;

    CallResultV<protocol::UserDefinedFunctionDescriptorPtrs>
    listUserDefinedFunctions(std::vector<std::string> * corrupted_keys = nullptr) const;

    /// Format schemas CRUD
    Error saveFormatSchema(const protocol::FormatSchemaDescriptor & desc, AppliedSequence applied_sn);
    Error deleteFormatSchema(const std::string & name, const std::string & format, AppliedSequence applied_sn);
    CallResultV<protocol::FormatSchemaDescriptorPtr> getFormatSchema(const std::string & name, const std::string & format) const;
    CallResultV<protocol::FormatSchemaDescriptorPtrs>
    getFormatSchema(const std::string & name, const std::string & format, size_t versions_requested) const;
    CallResultV<protocol::FormatSchemaDescriptorPtrs> listFormatSchemas(std::vector<std::string> * corrupted_keys = nullptr) const;

    /// Access Entity CRUD
    Error saveAccessEntity(const protocol::AccessEntityDescription & desc, AppliedSequence applied_sn);

    Error replaceAccessEntity(const protocol::AccessEntityDescription & desc, const DB::UUID & old_id, AppliedSequence applied_sn);

    CallResultV<protocol::AccessEntityDescriptionPtrs> listAccessEntities(std::vector<std::string> * corrupted_keys = nullptr) const;

    Error deleteAccessEntity(const DB::UUID & id, AppliedSequence applied_sn);

    /// Database CRUD
    Error saveDatabase(const protocol::DatabaseDescriptor & desc, AppliedSequence applied_sn);
    Error deleteDatabase(const std::string & name, AppliedSequence applied_sn);
    CallResultV<protocol::DatabaseDescriptorPtrs> getDatabase(const std::string & ns, size_t versions_requested) const;
    CallResultV<protocol::DatabaseDescriptorPtrs> listDatabases(std::vector<std::string> * corrupted_keys = nullptr) const;

    /// Disk CRUD
    Error saveDisk(const protocol::DiskDescriptor & desc, AppliedSequence applied_sn);
    Error deleteDisk(const std::string & name, AppliedSequence applied_sn);
    CallResultV<protocol::DiskDescriptorPtr> getDisk(const std::string & name) const;
    CallResultV<protocol::DiskDescriptorPtrs> listDisks(const std::string & name) const;
    CallResultV<protocol::DiskDescriptorPtrs> listDisks(std::vector<std::string> * corrupted_keys = nullptr) const;

    /// Storage Policy CRUD
    Error saveStoragePolicy(const protocol::StoragePolicyDescriptor & desc, AppliedSequence applied_sn);
    Error deleteStoragePolicy(const std::string & name, AppliedSequence applied_sn);
    CallResultV<protocol::StoragePolicyDescriptorPtr> getStoragePolicy(const std::string & name) const;
    CallResultV<protocol::StoragePolicyDescriptorPtrs> listStoragePolicies(const std::string & name) const;
    CallResultV<protocol::StoragePolicyDescriptorPtrs> listStoragePolicies(std::vector<std::string> * corrupted_keys = nullptr) const;

    /// Alert CRUD
    Error saveAlert(const protocol::AlertDescriptor & desc, AppliedSequence applied_sn);
    Error deleteAlert(const std::string & ns, const std::string & name, AppliedSequence applied_sn);
    Error deleteAlert(const std::string & ns, const std::string & name);
    CallResultV<protocol::AlertDescriptorPtr> getAlert(const std::string & ns, const std::string & name) const;
    CallResultV<protocol::AlertDescriptorPtrs> getAlert(const std::string & ns, const std::string & name, size_t versions_requested) const;
    CallResultV<protocol::AlertDescriptorPtrs> listAlerts(const std::string & ns) const;
    CallResultV<protocol::AlertDescriptorPtrs> listAlerts(std::vector<std::string> * corrupted_keys = nullptr) const;

    /// Task CRUD
    Error saveTask(const protocol::TaskDescriptor & desc, AppliedSequence applied_sn);
    Error deleteTask(const std::string & ns, const std::string & name, AppliedSequence applied_sn);
    Error deleteTask(const std::string & ns, const std::string & name);
    CallResultV<protocol::TaskDescriptorPtr> getTask(const std::string & ns, const std::string & name) const;
    CallResultV<protocol::TaskDescriptorPtrs> getTask(const std::string & ns, const std::string & name, size_t versions_requested) const;
    CallResultV<protocol::TaskDescriptorPtrs> listTasks(const std::string & ns) const;
    CallResultV<protocol::TaskDescriptorPtrs> listTasks(std::vector<std::string> * corrupted_keys = nullptr) const;

    /// Named Collection CRUD
    Error
    saveNamedCollection(const std::string & name, const protocol::NamedCollectionDescriptor & named_collection, AppliedSequence applied_sn);
    Error deleteNamedCollection(const std::string & name, AppliedSequence applied_sn);
    Error deleteNamedCollection(const std::string & name);
    CallResultV<protocol::NamedCollectionDescriptorPtr> getNamedCollection(const std::string & name) const;
    CallResultV<protocol::NamedCollectionDescriptorPtrs> getNamedCollection(const std::string & name, size_t versions_requested) const;
    CallResultV<std::vector<std::string>> listNamedCollections() const;

    /// Internal API, save / delete mv assignment from local metadb
    /// Materialized View Assignments
    Error saveMaterializedViewAssignment(const protocol::MaterializedViewAssignment & assignment, AppliedSequence applied_sn);
    Error deleteMaterializedViewAssignment(const Stream & mv);
    CallResultV<protocol::MaterializedViewAssignmentPtr> getMaterializedViewAssignment(const Stream & mv) const;
    CallResultV<protocol::MaterializedViewAssignmentPtrs>
    listMaterializedViewAssignments(std::vector<std::string> * corrupted_keys = nullptr) const;

    /// Last applied sequence is the last sequence number got applied
    /// to (application) meta kv.
    Error saveAppliedSequence(const AppliedSequence & applied_sn);
    CallResultV<AppliedSequence> loadAppliedSequence() const;
    CallResultV<AppliedSequence> lastAppliedSequence() const { return loadAppliedSequence(); }

    /// Pending request management
    /// Save a pending metadata request with sync write
    Error savePendingRequest(uint64_t sequence_number, const std::string & serialized_data);

    /// Delete a pending request after successful apply
    Error deletePendingRequest(uint64_t sequence_number);

    /// Iterate pending requests after given sequence number for recovery
    CallResultV<std::vector<std::pair<uint64_t, std::string>>> iteratePendingRequestsAfter(uint64_t after_sn) const;

    /// Cleanup old pending requests up to and including the given applied sequence number
    Error cleanupOldPendingRequests(uint64_t applied_sn);

private:
    Error mergeKeyValue(const std::string & key, const std::string & value, AppliedSequence applied_sn, std::string_view resource);
    Error deleteKey(const std::string & key, std::optional<AppliedSequence> applied_sn, std::string_view resource);

    template <typename T>
    CallResultV<std::shared_ptr<T>> getValue(const std::string & key, MetaKeySpace key_space) const;

    template <typename T>
    CallResultV<std::vector<std::shared_ptr<T>>> getValue(const std::string & key, MetaKeySpace key_space, size_t versions_requested) const;

    template <typename T>
    CallResultV<std::vector<std::shared_ptr<T>>>
    getValues(const std::string & prefix_start, MetaKeySpace key_space, std::vector<std::string> * corrupted_keys) const;

private:
    /// StreamIDShard for metalog
    StreamIDShard stream_shard;
    std::string applied_sn_key;

    std::unique_ptr<rocksdb::DB> meta_db;

    rocksdb::ColumnFamilyHandle * default_cf_handle = nullptr;


    LoggerPtr logger;
};
}
