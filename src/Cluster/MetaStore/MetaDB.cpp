#include <Cluster/Common/EncodeMetaKey.h>
/// #include <Cluster/Common/KeyValueSnapshotBatch.h> - Not needed
#include <Cluster/Common/serde.h>
#include <Cluster/MetaStore/MetaDB.h>
#include <Cluster/MetaStore/MultipleVersionMergeOperator.h>
#include <Cluster/Rocks/mapRocksStatus.h>

#include <utility>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/PrefixTreeEncode.h>
#include <Common/logger_useful.h>

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>

namespace DB::ErrorCodes
{
extern const int CANNOT_OPEN_DATABASE;
extern const int OK;
extern const int RESOURCE_ALREADY_EXISTS;
extern const int FAILED_TO_TAKE_SNAPSHOT;
}

namespace cluster::meta
{
namespace
{
const std::string META_DB_DIR = "kv";

rocksdb::Options getRocksDBOptions()
{
    rocksdb::Options db_options;
    db_options.num_levels = 3;
    db_options.create_if_missing = true;
    db_options.create_missing_column_families = true;

    /// db_options.max_total_wal_size = 1024;
    /// db_options.IncreaseParallelism();
    /// db_options.OptimizeLevelStyleCompaction();

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    db_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    return db_options;
}
}

MetaDB::MetaDB(const std::filesystem::path & meta_dir, const StreamIDShard & stream_shard_, size_t keep_versions, LoggerPtr logger_)
    : stream_shard(stream_shard_), applied_sn_key(encodeMetaAppliedSequenceKey(stream_shard)), logger(logger_)
{
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.reserve(1);

    /// Support multiple version schema
    rocksdb::ColumnFamilyOptions stream_cf_options;
    stream_cf_options.merge_operator = std::make_shared<MultipleVersionMergeOperator>(keep_versions, logger_);

    column_families.push_back(rocksdb::ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, stream_cf_options));

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    cf_handles.reserve(column_families.size());

    rocksdb::DB * db = nullptr;

    if (auto status = rocksdb::DB::Open(getRocksDBOptions(), meta_dir / META_DB_DIR, column_families, &cf_handles, &db); !status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_DATABASE, "Failed to open metadb {}", status.ToString());

    meta_db.reset(db);

    default_cf_handle = cf_handles[0];

    LOG_INFO(logger, "Init metastore in dir={}", meta_dir.c_str());
}

MetaDB::~MetaDB()
{
    LOG_INFO(logger, "Closing metadb");

    if (default_cf_handle != nullptr)
    {
        if (auto status = meta_db->DestroyColumnFamilyHandle(default_cf_handle); !status.ok())
            LOG_ERROR(logger, "Failed to destroy column family handle, error={}", status.ToString());
    }

    if (auto status = meta_db->Close(); !status.ok())
        LOG_ERROR(logger, "Failed to close metadb {}", status.ToString());

    LOG_INFO(logger, "Closed metadb");
}

Error MetaDB::deleteStream(const std::string & ns, const std::string & stream, AppliedSequence applied_sn)
{
    return deleteKey(encodeMetaStreamKey(ns, stream), applied_sn, "stream");
}

Error MetaDB::renameStream(const std::string & old_name, const protocol::StreamDescriptor & desc, AppliedSequence applied_sn)
{
    auto existing_key = encodeMetaStreamKey(desc.stream.ns, old_name);
    auto new_key = encodeMetaStreamKey(desc.stream.ns, desc.stream.name);
    assert(existing_key != new_key);

    auto data = cluster::serialize<std::string>(desc, /*version=*/1);

    rocksdb::WriteBatch batch;
    batch.Delete(default_cf_handle, existing_key);
    batch.Put(default_cf_handle, new_key, data);

    auto applied_sn_data = cluster::serialize<std::string>(applied_sn, /*version=*/1);
    batch.Put(default_cf_handle, applied_sn_key, applied_sn_data);

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    if (auto status = meta_db->Write(write_options, &batch); status.ok())
    {
        return {};
    }
    else
    {
        LOG_ERROR(logger, "Failed to rename stream={} error={}", existing_key, status.ToString());
        return Error(mapRocksStatus(status), status.ToString());
    }
}

Error MetaDB::saveStream(const protocol::StreamDescriptor & desc, AppliedSequence applied_sn)
{
    return mergeKeyValue(
        encodeMetaStreamKey(desc.stream.ns, desc.stream.name), cluster::serialize<std::string>(desc, /*version=*/1), applied_sn, "stream");
}

CallResultV<protocol::StreamDescriptorPtr> MetaDB::getStream(const std::string & ns, const std::string & stream) const
{
    return getValue<protocol::StreamDescriptor>(encodeMetaStreamKey(ns, stream), MetaKeySpace::Stream);
}

CallResultV<protocol::StreamDescriptorPtrs>
MetaDB::getStream(const std::string & ns, const std::string & stream, size_t versions_requested) const
{
    return getValue<protocol::StreamDescriptor>(encodeMetaStreamKey(ns, stream), MetaKeySpace::Stream, versions_requested);
}

CallResultV<protocol::StreamDescriptorPtrs> MetaDB::listStreams(const std::string & ns) const
{
    return getValues<protocol::StreamDescriptor>(encodeMetaStreamKey(ns), MetaKeySpace::Stream, /*corrupted_keys=*/nullptr);
}

CallResultV<protocol::StreamDescriptorPtrs> MetaDB::listStreams(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::StreamDescriptor>(encodeMetaStreamKey(), MetaKeySpace::Stream, corrupted_keys);
}

Error MetaDB::saveUserDefinedFunction(const protocol::UserDefinedFunctionDescriptor & desc, AppliedSequence applied_sn)
{
    return mergeKeyValue(encodeMetaUDFKey(desc.name), cluster::serialize<std::string>(desc, /*version=*/1), applied_sn, "udf");
}

Error MetaDB::deleteUserDefinedFunction(const std::string & name, AppliedSequence applied_sn)
{
    return deleteKey(encodeMetaUDFKey(name), applied_sn, "udf");
}

CallResultV<protocol::UserDefinedFunctionDescriptorPtr> MetaDB::getUserDefinedFunction(const std::string & name) const
{
    return getValue<protocol::UserDefinedFunctionDescriptor>(encodeMetaUDFKey(name), MetaKeySpace::UDF);
}

CallResultV<protocol::UserDefinedFunctionDescriptorPtrs>
MetaDB::getUserDefinedFunction(const std::string & name, size_t versions_requested) const
{
    return getValue<protocol::UserDefinedFunctionDescriptor>(encodeMetaUDFKey(name), MetaKeySpace::UDF, versions_requested);
}

CallResultV<protocol::UserDefinedFunctionDescriptorPtrs> MetaDB::listUserDefinedFunctions(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::UserDefinedFunctionDescriptor>(encodeMetaUDFKey(), MetaKeySpace::UDF, corrupted_keys);
}

Error MetaDB::saveFormatSchema(const protocol::FormatSchemaDescriptor & desc, AppliedSequence applied_sn)
{
    return mergeKeyValue(
        encodeMetaFormatSchemaKey(desc.name, desc.format),
        cluster::serialize<std::string>(desc, /*version=*/1),
        applied_sn,
        "format_schema");
}

Error MetaDB::deleteFormatSchema(const std::string & name, const std::string & format, AppliedSequence applied_sn)
{
    return deleteKey(encodeMetaFormatSchemaKey(name, format), applied_sn, "format_schema");
}

CallResultV<protocol::FormatSchemaDescriptorPtr> MetaDB::getFormatSchema(const std::string & name, const std::string & format) const
{
    return getValue<protocol::FormatSchemaDescriptor>(encodeMetaFormatSchemaKey(name, format), MetaKeySpace::FormatSchema);
}

CallResultV<protocol::FormatSchemaDescriptorPtrs>
MetaDB::getFormatSchema(const std::string & name, const std::string & format, size_t versions_requested) const
{
    return getValue<protocol::FormatSchemaDescriptor>(
        encodeMetaFormatSchemaKey(name, format), MetaKeySpace::FormatSchema, versions_requested);
}

CallResultV<protocol::FormatSchemaDescriptorPtrs> MetaDB::listFormatSchemas(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::FormatSchemaDescriptor>(encodeMetaFormatSchemaKey(), MetaKeySpace::FormatSchema, corrupted_keys);
}

Error MetaDB::saveAccessEntity(const protocol::AccessEntityDescription & desc, AppliedSequence applied_sn)
{
    rocksdb::WriteBatch batch;
    batch.Put(default_cf_handle, encodeMetaAccessEntityKey(DB::toString(desc.id)), cluster::serialize<std::string>(desc, /*version=*/1));
    batch.Put(default_cf_handle, applied_sn_key, cluster::serialize<std::string>(applied_sn, /*version=*/1));

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = meta_db->Write(write_options, &batch);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to save access entity: desc={} error={}", desc.string(), status.ToString());
        return Error(mapRocksStatus(status), status.ToString());
    }

    return {};
}

Error MetaDB::replaceAccessEntity(const protocol::AccessEntityDescription & desc, const DB::UUID & old_id, AppliedSequence applied_sn)
{
    /// Replace the old Access Entity which has the same name as new one.
    rocksdb::WriteBatch batch;
    batch.Put(default_cf_handle, encodeMetaAccessEntityKey(DB::toString(desc.id)), cluster::serialize<std::string>(desc, /*version=*/1));
    batch.Delete(default_cf_handle, encodeMetaAccessEntityKey(DB::toString(old_id)));

    auto applied_sn_data = cluster::serialize<std::string>(applied_sn, /*version=*/1);
    batch.Put(default_cf_handle, applied_sn_key, applied_sn_data);

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = meta_db->Write(write_options, &batch);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to replace access entity: desc={{{}}} error={}", desc.string(), status.ToString());
        return Error(mapRocksStatus(status), status.ToString());
    }

    return {};
}

CallResultV<protocol::AccessEntityDescriptionPtrs> MetaDB::listAccessEntities(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::AccessEntityDescription>(encodeMetaAccessEntityKey(), MetaKeySpace::AccessEntity, corrupted_keys);
}

Error MetaDB::deleteAccessEntity(const DB::UUID & id, AppliedSequence applied_sn)
{
    return deleteKey(encodeMetaAccessEntityKey(DB::toString(id)), applied_sn, "access_entity");
}


Error MetaDB::saveDatabase(const protocol::DatabaseDescriptor & desc, AppliedSequence applied_sn)
{
    rocksdb::WriteBatch batch;
    batch.Put(default_cf_handle, encodeMetaDatabaseKey(desc.name), cluster::serialize<std::string>(desc, /*version=*/1));
    batch.Put(default_cf_handle, applied_sn_key, cluster::serialize<std::string>(applied_sn, /*version=*/1));

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = meta_db->Write(write_options, &batch);
    if (!status.ok())
        return Error(mapRocksStatus(status), status.ToString());

    return {};
}

Error MetaDB::deleteDatabase(const std::string & name, AppliedSequence applied_sn)
{
    return deleteKey(encodeMetaDatabaseKey(name), applied_sn, "database");
}

CallResultV<protocol::DatabaseDescriptorPtrs> MetaDB::getDatabase(const std::string & ns, size_t versions_requested) const
{
    return getValue<protocol::DatabaseDescriptor>(encodeMetaDatabaseKey(ns), MetaKeySpace::Database, versions_requested);
}

CallResultV<protocol::DatabaseDescriptorPtrs> MetaDB::listDatabases(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::DatabaseDescriptor>(encodeMetaDatabaseKey(), MetaKeySpace::Database, corrupted_keys);
}

Error MetaDB::saveDisk(const protocol::DiskDescriptor & desc, AppliedSequence applied_sn)
{
    rocksdb::WriteBatch batch;
    batch.Put(default_cf_handle, encodeMetaDiskKey(desc.name), cluster::serialize<std::string>(desc, /*version=*/1));
    batch.Put(default_cf_handle, applied_sn_key, cluster::serialize<std::string>(applied_sn, /*version=*/1));

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = meta_db->Write(write_options, &batch);
    if (!status.ok())
        return Error(mapRocksStatus(status), status.ToString());

    return {};
}

Error MetaDB::deleteDisk(const std::string & name, AppliedSequence applied_sn)
{
    return deleteKey(encodeMetaDiskKey(name), applied_sn, "disk");
}

CallResultV<protocol::DiskDescriptorPtr> MetaDB::getDisk(const std::string & name) const
{
    return getValue<protocol::DiskDescriptor>(encodeMetaDiskKey(name), MetaKeySpace::Disk);
}

CallResultV<protocol::DiskDescriptorPtrs> MetaDB::listDisks(const std::string & name) const
{
    if (name.empty())
        return getValues<protocol::DiskDescriptor>(encodeMetaDiskKey(), MetaKeySpace::Disk, /*corrupted_keys=*/nullptr);
    else
        return getValues<protocol::DiskDescriptor>(encodeMetaDiskKey(name), MetaKeySpace::Disk, /*corrupted_keys=*/nullptr);
}

CallResultV<protocol::DiskDescriptorPtrs> MetaDB::listDisks(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::DiskDescriptor>(encodeMetaDiskKey(), MetaKeySpace::Disk, corrupted_keys);
}

Error MetaDB::saveStoragePolicy(const protocol::StoragePolicyDescriptor & desc, AppliedSequence applied_sn)
{
    rocksdb::WriteBatch batch;
    batch.Put(default_cf_handle, encodeMetaStoragePolicyKey(desc.name), cluster::serialize<std::string>(desc, /*version=*/1));
    batch.Put(default_cf_handle, applied_sn_key, cluster::serialize<std::string>(applied_sn, /*version=*/1));

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = meta_db->Write(write_options, &batch);
    if (!status.ok())
    {
        return Error(mapRocksStatus(status), status.ToString());
    }

    return {};
}

Error MetaDB::deleteStoragePolicy(const std::string & name, AppliedSequence applied_sn)
{
    return deleteKey(encodeMetaStoragePolicyKey(name), applied_sn, "storage_policy");
}

CallResultV<protocol::StoragePolicyDescriptorPtr> MetaDB::getStoragePolicy(const std::string & name) const
{
    return getValue<protocol::StoragePolicyDescriptor>(encodeMetaStoragePolicyKey(name), MetaKeySpace::StoragePolicy);
}

CallResultV<protocol::StoragePolicyDescriptorPtrs> MetaDB::listStoragePolicies(const std::string & name) const
{
    if (name.empty())
        return getValues<protocol::StoragePolicyDescriptor>(
            encodeMetaStoragePolicyKey(), MetaKeySpace::StoragePolicy, /*corrupted_keys=*/nullptr);
    else
        return getValues<protocol::StoragePolicyDescriptor>(
            encodeMetaStoragePolicyKey(name), MetaKeySpace::StoragePolicy, /*corrupted_keys=*/nullptr);
}

CallResultV<protocol::StoragePolicyDescriptorPtrs> MetaDB::listStoragePolicies(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::StoragePolicyDescriptor>(encodeMetaStoragePolicyKey(), MetaKeySpace::StoragePolicy, corrupted_keys);
}

Error MetaDB::saveMaterializedViewAssignment(
    const cluster::protocol::MaterializedViewAssignment & assignment, cluster::AppliedSequence applied_sn)
{
    rocksdb::WriteBatch batch;
    batch.Put(
        default_cf_handle,
        encodeMetaMaterializedViewAssignmentKey(assignment.mv.ns, assignment.mv.name),
        cluster::serialize<std::string>(assignment, /*version=*/1));
    batch.Put(default_cf_handle, applied_sn_key, cluster::serialize<std::string>(applied_sn, /*version=*/1));

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = meta_db->Write(write_options, &batch);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to save MaterializedView assignment in meta store {}", status.ToString());
        return Error(mapRocksStatus(status), status.ToString());
    }

    return {};
}

Error MetaDB::saveAlert(const protocol::AlertDescriptor & desc, AppliedSequence applied_sn)
{
    return mergeKeyValue(encodeMetaAlertKey(desc.ns, desc.name), cluster::serialize<std::string>(desc, /*version=*/1), applied_sn, "alert");
}

Error MetaDB::deleteAlert(const std::string & ns, const std::string & name, AppliedSequence applied_sn)
{
    return deleteKey(encodeMetaAlertKey(ns, name), applied_sn, "alert");
}

Error MetaDB::deleteAlert(const std::string & ns, const std::string & name)
{
    return deleteKey(encodeMetaAlertKey(ns, name), std::nullopt, "alert");
}

CallResultV<protocol::AlertDescriptorPtr> MetaDB::getAlert(const std::string & ns, const std::string & name) const
{
    return getValue<protocol::AlertDescriptor>(encodeMetaAlertKey(ns, name), MetaKeySpace::Alert);
}

CallResultV<protocol::AlertDescriptorPtrs>
MetaDB::getAlert(const std::string & ns, const std::string & name, size_t versions_requested) const
{
    return getValue<protocol::AlertDescriptor>(encodeMetaAlertKey(ns, name), MetaKeySpace::Alert, versions_requested);
}

CallResultV<protocol::AlertDescriptorPtrs> MetaDB::listAlerts(const std::string & ns) const
{
    return getValues<protocol::AlertDescriptor>(encodeMetaAlertKey(ns), MetaKeySpace::Alert, /*corrupted_keys=*/nullptr);
}

CallResultV<protocol::AlertDescriptorPtrs> MetaDB::listAlerts(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::AlertDescriptor>(encodeMetaAlertKey(), MetaKeySpace::Alert, corrupted_keys);
}

Error MetaDB::saveTask(const protocol::TaskDescriptor & desc, AppliedSequence applied_sn)
{
    return mergeKeyValue(encodeMetaTaskKey(desc.ns, desc.name), cluster::serialize<std::string>(desc, /*version=*/1), applied_sn, "task");
}

Error MetaDB::deleteTask(const std::string & ns, const std::string & name, AppliedSequence applied_sn)
{
    return deleteKey(encodeMetaTaskKey(ns, name), applied_sn, "task");
}

Error MetaDB::deleteTask(const std::string & ns, const std::string & name)
{
    return deleteKey(encodeMetaTaskKey(ns, name), std::nullopt, "task");
}

CallResultV<protocol::TaskDescriptorPtr> MetaDB::getTask(const std::string & ns, const std::string & name) const
{
    return getValue<protocol::TaskDescriptor>(encodeMetaTaskKey(ns, name), MetaKeySpace::Task);
}

CallResultV<protocol::TaskDescriptorPtrs> MetaDB::getTask(const std::string & ns, const std::string & name, size_t versions_requested) const
{
    return getValue<protocol::TaskDescriptor>(encodeMetaTaskKey(ns, name), MetaKeySpace::Task, versions_requested);
}

CallResultV<protocol::TaskDescriptorPtrs> MetaDB::listTasks(const std::string & ns) const
{
    return getValues<protocol::TaskDescriptor>(encodeMetaTaskKey(ns), MetaKeySpace::Task, /*corrupted_keys=*/nullptr);
}

CallResultV<protocol::TaskDescriptorPtrs> MetaDB::listTasks(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::TaskDescriptor>(encodeMetaTaskKey(), MetaKeySpace::Task, corrupted_keys);
}

Error MetaDB::deleteMaterializedViewAssignment(const cluster::Stream & mv)
{
    return deleteKey(encodeMetaMaterializedViewAssignmentKey(mv.ns, mv.name), {}, "MaterializedViewAssignment");
}

CallResultV<protocol::MaterializedViewAssignmentPtr> MetaDB::getMaterializedViewAssignment(const cluster::Stream & mv) const
{
    return getValue<protocol::MaterializedViewAssignment>(
        encodeMetaMaterializedViewAssignmentKey(mv.ns, mv.name), MetaKeySpace::MaterializedViewAssignment);
}

CallResultV<protocol::MaterializedViewAssignmentPtrs>
MetaDB::listMaterializedViewAssignments(std::vector<std::string> * corrupted_keys) const
{
    return getValues<protocol::MaterializedViewAssignment>(
        encodeMetaMaterializedViewAssignmentKey(), MetaKeySpace::MaterializedViewAssignment, corrupted_keys);
}


Error MetaDB::mergeKeyValue(
    const std::string & key, const std::string & value, cluster::AppliedSequence applied_sn, std::string_view resource)
{
    rocksdb::WriteBatch batch;
    batch.Merge(default_cf_handle, key, value);

    auto applied_sn_data = cluster::serialize<std::string>(applied_sn, /*version=*/1);
    batch.Put(default_cf_handle, applied_sn_key, applied_sn_data);

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = meta_db->Write(write_options, &batch);
    if (status.ok())
    {
        return {};
    }
    else
    {
        LOG_ERROR(logger, "Failed to create {}={} error={}", resource, key, status.ToString());
        return Error(mapRocksStatus(status), status.ToString());
    }
}

Error MetaDB::deleteKey(const std::string & key, std::optional<AppliedSequence> applied_sn, std::string_view resource)
{
    rocksdb::WriteBatch batch;
    batch.Delete(default_cf_handle, key);

    if (applied_sn)
    {
        auto applied_sn_data = cluster::serialize<std::string>(*applied_sn, /*version=*/1);
        batch.Put(default_cf_handle, applied_sn_key, applied_sn_data);
    }

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    if (auto status = meta_db->Write(write_options, &batch); status.ok())
    {
        return {};
    }
    else
    {
        LOG_ERROR(logger, "Failed to delete {}={}, error={}", resource, key, status.ToString());
        return Error(mapRocksStatus(status), status.ToString());
    }
}


/// Get single value for key
template <typename T>
CallResultV<std::shared_ptr<T>> MetaDB::getValue(const std::string & key, MetaKeySpace key_space) const
{
    std::string rvalue;
    auto status = meta_db->Get(rocksdb::ReadOptions{}, default_cf_handle, key, &rvalue);
    if (!status.ok())
    {
        if (!status.IsNotFound())
            LOG_ERROR(logger, "Failed to read MetaDB: key={} status={}", key, status.ToString());
        return CallResultV<std::shared_ptr<T>>(mapRocksStatus(status), status.ToString());
    }

    try
    {
        return CallResultV{MultipleVersionEncoder::decodeLatest<T>(key_space, key, rvalue, /*version=*/1)};
    }
    catch (const Poco::Exception & e)
    {
        std::string text = fmt::format("Failed to decode value: key={} error={}", key, e.displayText());
        LOG_ERROR(logger, "{}", text);
        return CallResultV<std::shared_ptr<T>>{e.code(), std::move(text)};
    }
}

/// Get values for requested versions of the key
template <typename T>
CallResultV<std::vector<std::shared_ptr<T>>>
MetaDB::getValue(const std::string & key, MetaKeySpace key_space, size_t versions_requested) const
{
    std::string rvalue;
    auto status = meta_db->Get(rocksdb::ReadOptions{}, default_cf_handle, key, &rvalue);
    if (!status.ok())
    {
        if (!status.IsNotFound())
            LOG_ERROR(logger, "Failed to read MetaDB: key={} status={}", key, status.ToString());
        return CallResultV<std::vector<std::shared_ptr<T>>>(mapRocksStatus(status), status.ToString());
    }

    try
    {
        return CallResultV{MultipleVersionEncoder::decode<T>(key_space, key, rvalue, versions_requested, /*version=*/1)};
    }
    catch (const Poco::Exception & e)
    {
        std::string text = fmt::format("Failed to decode value: key={} error={}", key, e.displayText());
        LOG_ERROR(logger, "{}", text);
        return CallResultV<std::vector<std::shared_ptr<T>>>{e.code(), std::move(text)};
    }
}

/// Get values for a key range
template <typename T>
CallResultV<std::vector<std::shared_ptr<T>>>
MetaDB::getValues(const std::string & prefix_start, MetaKeySpace key_space, std::vector<std::string> * corrupted_keys) const
{
    std::string prefix_end = prefix_start;
    DB::PrefixTreeEncode::keyPrefixEnd(prefix_end);

    rocksdb::Slice prefix_start_slice = prefix_start;
    rocksdb::Slice prefix_end_slice = prefix_end;

    rocksdb::ReadOptions options;
    options.auto_prefix_mode = true;
    options.iterate_upper_bound = &prefix_end_slice;

    std::unique_ptr<rocksdb::Iterator> iter{meta_db->NewIterator(options, default_cf_handle)};

    CallResultV<std::vector<std::shared_ptr<T>>> result;

    Error last_err;

    iter->Seek(prefix_start_slice);
    while (iter->Valid())
    {
        try
        {
            result.result.push_back(MultipleVersionEncoder::decodeLatest<T>(
                key_space, iter->key().ToStringView(), iter->value().ToStringView(), /*version=*/1));
        }
        catch (const Poco::Exception & e)
        {
            if (corrupted_keys)
                corrupted_keys->push_back(iter->key().ToString());

            auto text = fmt::format("Failed to decode value: key={} error={}", iter->key().ToStringView(), e.displayText());
            last_err = Error(e.code(), text);
            LOG_ERROR(logger, "{}, will skip it", text);
            /// break;
        }
        iter->Next();
    }

    if (result.result.empty() && last_err.hasError())
        result.err.swap(last_err);

    return result;
}

Error MetaDB::savePendingRequest(uint64_t sequence_number, const std::string & serialized_data)
{
    std::string key = encodeMetaPendingRequestKey(sequence_number);

    rocksdb::WriteOptions write_options;
    write_options.sync = true; // MUST use sync write as per requirements

    auto status = meta_db->Put(write_options, default_cf_handle, key, serialized_data);
    if (status.ok())
    {
        return {};
    }
    else
    {
        LOG_ERROR(logger, "Failed to save pending request sn={} error={}", sequence_number, status.ToString());
        return Error(mapRocksStatus(status), status.ToString());
    }
}

Error MetaDB::deletePendingRequest(uint64_t sequence_number)
{
    std::string key = encodeMetaPendingRequestKey(sequence_number);

    rocksdb::WriteOptions write_options;
    write_options.sync = false; // Best-effort delete, no need for sync

    auto status = meta_db->Delete(write_options, default_cf_handle, key);
    if (status.ok() || status.IsNotFound())
    {
        return {};
    }
    else
    {
        LOG_ERROR(logger, "Failed to delete pending request sn={} error={}", sequence_number, status.ToString());
        return Error(mapRocksStatus(status), status.ToString());
    }
}

CallResultV<std::vector<std::pair<uint64_t, std::string>>> MetaDB::iteratePendingRequestsAfter(uint64_t after_sn) const
{
    std::vector<std::pair<uint64_t, std::string>> result;

    /// Start key for iteration - one past the given sequence number
    std::string start_key = encodeMetaPendingRequestKey(after_sn + 1);
    
    /// Prefix for PendingRequest keyspace
    std::string prefix;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::PendingRequest), prefix);
    
    /// Calculate prefix end for iteration boundary
    std::string prefix_end = prefix;
    DB::PrefixTreeEncode::keyPrefixEnd(prefix_end);

    rocksdb::ReadOptions read_options;
    read_options.auto_prefix_mode = true;
    rocksdb::Slice prefix_end_slice(prefix_end);
    read_options.iterate_upper_bound = &prefix_end_slice;
    
    std::unique_ptr<rocksdb::Iterator> iter{meta_db->NewIterator(read_options, default_cf_handle)};

    for (iter->Seek(start_key); iter->Valid(); iter->Next())
    {
        std::string_view key_view = iter->key().ToStringView();
        
        /// Check if still in PendingRequest keyspace
        if (!key_view.starts_with(prefix))
            break;
            
        /// Decode sequence number from binary encoded key
        std::string_view key_remainder = key_view.substr(prefix.size());
        try
        {
            uint64_t sn = DB::PrefixTreeEncode::decodeUInt64Ascending(key_remainder);
            result.emplace_back(sn, iter->value().ToString());
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(logger, "Failed to decode sequence number from key, error={}", e.what());
            /// Skip corrupted key
        }
    }

    if (!iter->status().ok())
    {
        LOG_ERROR(logger, "Failed to iterate pending requests, error={}", iter->status().ToString());
        return CallResultV<std::vector<std::pair<uint64_t, std::string>>>{mapRocksStatus(iter->status()), iter->status().ToString()};
    }

    return CallResultV{std::move(result)};
}

Error MetaDB::cleanupOldPendingRequests(uint64_t applied_sn)
{
    /// Prefix for PendingRequest keyspace
    std::string prefix;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::PendingRequest), prefix);
    
    /// End key for cleanup - one past the applied sequence number
    std::string end_key = encodeMetaPendingRequestKey(applied_sn + 1);

    rocksdb::WriteBatch batch;
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iter{meta_db->NewIterator(read_options, default_cf_handle)};

    size_t deleted_count = 0;
    for (iter->Seek(prefix); iter->Valid() && iter->key().compare(end_key) < 0; iter->Next())
    {
        std::string_view key_view = iter->key().ToStringView();
        
        /// Check if still in PendingRequest keyspace
        if (!key_view.starts_with(prefix))
            break;
            
        batch.Delete(default_cf_handle, iter->key());
        deleted_count++;
    }

    if (!iter->status().ok())
    {
        LOG_ERROR(logger, "Failed to iterate for cleanup, error={}", iter->status().ToString());
        return Error(mapRocksStatus(iter->status()), iter->status().ToString());
    }

    if (deleted_count > 0)
    {
        rocksdb::WriteOptions write_options;
        write_options.sync = false; // Best-effort cleanup, no need for sync

        auto status = meta_db->Write(write_options, &batch);
        if (status.ok())
        {
            LOG_INFO(logger, "Cleaned up {} old pending requests up to sn={}", deleted_count, applied_sn);
            return {};
        }
        else
        {
            LOG_ERROR(logger, "Failed to cleanup old pending requests, error={}", status.ToString());
            return Error(mapRocksStatus(status), status.ToString());
        }
    }

    return {};
}


cluster::Error cluster::meta::MetaDB::saveAppliedSequence(const cluster::AppliedSequence & applied_sn)
{
    auto applied_sn_data = cluster::serialize<std::string>(applied_sn, /*version=*/1);

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = meta_db->Put(write_options, default_cf_handle, applied_sn_key, applied_sn_data);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to save applied sequence: {}", status.ToString());
        return Error(mapRocksStatus(status), status.ToString());
    }

    /// Best-effort cleanup of pending request after successful apply
    deletePendingRequest(applied_sn.sn);

    return {};
}

cluster::CallResultV<cluster::AppliedSequence> cluster::meta::MetaDB::loadAppliedSequence() const
{
    std::string rvalue;
    auto status = meta_db->Get(rocksdb::ReadOptions{}, default_cf_handle, applied_sn_key, &rvalue);
    if (!status.ok())
    {
        if (status.IsNotFound())
        {
            /// Return empty result (no error) for NotFound case
            /// This indicates no previous applied sequence exists (first startup)
            return cluster::CallResultV<cluster::AppliedSequence>{};
        }
        
        LOG_ERROR(logger, "Failed to load applied sequence: {}", status.ToString());
        return cluster::CallResultV<cluster::AppliedSequence>(mapRocksStatus(status), status.ToString());
    }

    try
    {
        auto applied_sn = cluster::parse<cluster::AppliedSequence, false>(rvalue, /*version=*/1);
        return cluster::CallResultV<cluster::AppliedSequence>{applied_sn};
    }
    catch (const Poco::Exception & e)
    {
        std::string text = fmt::format("Failed to decode applied sequence: error={}", e.displayText());
        LOG_ERROR(logger, "{}", text);
        return cluster::CallResultV<cluster::AppliedSequence>{e.code(), std::move(text)};
    }
}
}
