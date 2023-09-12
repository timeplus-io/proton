#include <Coordination/MetaStateMachine.h>

#include <Coordination/KVNamespaceAndPrefixHelper.h>
#include <Coordination/KVRequest.h>
#include <Coordination/KVResponse.h>
#include <Coordination/MetaSnapshotManager.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>

#include <future>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"

#define _TP_SYSTEM_COLUMN_FAMILY_NAME (rocksdb::kDefaultColumnFamilyName)

#pragma clang diagnostic pop

namespace DB
{
[[maybe_unused]] const std::string META_LOG_INDEX_KEY = "_metastore_log_index";

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ROCKSDB_ERROR;
    extern const int STD_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
}

namespace
{
    Coordination::KVRequestPtr parseKVRequest(nuraft::buffer & data)
    {
        using namespace Coordination;

        ReadBufferFromNuraftBuffer buffer(data);

        KVRequestPtr request = KVRequest::read(buffer);
        return request;
    }

    nuraft::ptr<nuraft::buffer> getBufferFromKVResponse(const Coordination::KVResponsePtr & response) noexcept
    {
        DB::WriteBufferFromNuraftBuffer buf;
        response->write(buf);
        return buf.getBuffer();
    }

    std::vector<rocksdb::Slice> convertStringsToSlices(const std::vector<std::string> & strs)
    {
        std::vector<rocksdb::Slice> slices;
        slices.reserve(strs.size());
        for (const auto & str : strs)
            slices.emplace_back(str);
        return slices;
    }
}

MetaStateMachine::MetaStateMachine(
    MetaSnapshotsQueue & snapshots_queue_,
    const std::string & snapshots_path_,
    const std::string & db_path_,
    const CoordinationSettingsPtr & coordination_settings_)
    : rocksdb_dir(db_path_)
    , coordination_settings(coordination_settings_)
    , snapshot_manager(snapshots_path_, coordination_settings->snapshots_to_keep)
    , snapshots_queue(snapshots_queue_)
    , last_committed_idx(0)
    , log(&Poco::Logger::get("MetaStateMachine"))
{
}

void MetaStateMachine::init()
{
    initDB();

    /// Do everything without mutexes, no other threads exist.
    LOG_INFO(log, "Totally have {} snapshots", snapshot_manager.totalSnapshots());

    //    bool has_snapshots = snapshot_manager.totalSnapshots() != 0;
    std::string idx_value;
    rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), system_cf_handler, META_LOG_INDEX_KEY, &idx_value);
    if (status.ok())
    {
        assert(idx_value.size() == sizeof(uint64_t));
        last_committed_idx = *reinterpret_cast<uint64_t*>(idx_value.data());
    }

    LOG_INFO(log, "last committed log index {}", last_committed_idx);

    while (snapshot_manager.totalSnapshots() != 0)
    {
        auto latest_log_idx = snapshot_manager.getLatestSnapshotIndex();
        try
        {
            latest_snapshot_buf = snapshot_manager.deserializeSnapshotBufferFromDisk(latest_log_idx);
            latest_snapshot_meta = snapshot_manager.deserializeSnapshotFromBuffer(latest_snapshot_buf);

            LOG_DEBUG(
                log,
                "Trying to load snapshot_meta info from snapshot up to log index {}",
                latest_snapshot_meta->snapshot_meta->get_last_log_idx());

            break;
        }
        catch (const DB::Exception & ex)
        {
            LOG_ERROR(
                log,
                "Failed to load from snapshot with index {}, with error {}, will remove it from disk",
                latest_log_idx,
                ex.displayText());

            snapshot_manager.removeSnapshot(latest_log_idx);
        }
    }
}

/// This func called by async thread, if there is exception, it's cause to crash that the whole process exit.
/// So, we must to ensure the success of commit as much as possible
nuraft::ptr<nuraft::buffer> MetaStateMachine::commit(const uint64_t log_idx, nuraft::buffer & data)
{
    LOG_DEBUG(log, "commit index {}", log_idx);

    auto request = parseKVRequest(data);
    const auto & op = request->getOpNum();
    auto response = Coordination::KVResponseFactory::instance().get(request);

    switch (op)
    {
        case Coordination::KVOpNum::MULTIPUT:
        {
            auto write_batch = [this](const auto & kv_pairs, const auto & cf) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                rocksdb::Status status;
                rocksdb::WriteBatch batch;

                auto cf_handler = getOrCreateColumnFamilyHandler(cf);
                for (const auto & [key, value] : kv_pairs)
                {
                    status = batch.Put(cf_handler, key, value);
                    if (!status.ok())
                        return status;
                }
                return rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
            };

            auto req = request->as<const Coordination::KVMultiPutRequest>();
            auto status = write_batch(req->kv_pairs, req->column_family);
            if (!status.ok())
            {
                /// NOTICE: There is incomplete rollback operation in nuraft, so it will cause the system to exit.
                throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
            }
            break;
        }
        case Coordination::KVOpNum::MULTIGET:
        {
            auto req = request->as<const Coordination::KVMultiGetRequest>();
            auto resp = response->as<Coordination::KVMultiGetResponse>();
            const auto & slices_keys = convertStringsToSlices(req->keys);
            std::vector<String> values;

            /// Return error if not column family
            auto cf_handler = tryGetColumnFamilyHandler(req->column_family);
            if (!cf_handler)
            {
                resp->code = ErrorCodes::ROCKSDB_ERROR;
                resp->msg = "RocksDB read error: NotFound: column family '" + req->column_family + "' doesn't exists.";
                break;
            }

            auto statuses = rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), {slices_keys.size(), cf_handler}, slices_keys, &values);
            for (size_t i = 0; i < statuses.size(); ++i)
            {
                resp->kv_pairs.emplace_back(req->keys[i], values[i]);
                if (!statuses[i].ok())
                {
                    /// NOTICE: There is incomplete rollback operation in nuraft, so it will cause the system to exit.
                    // throw Exception("RocksDB read error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);

                    /// To avoid to exit for exception, even if the read failed, we also consider this commit successful,
                    /// and return error status.
                    resp->code = ErrorCodes::ROCKSDB_ERROR;
                    resp->msg = fmt::format("RocksDB get '{}' error: {}", req->keys[i], statuses[i].ToString());
                    break;
                }
            }
            break;
        }
        case Coordination::KVOpNum::MULTIDELETE:
        {
            auto req = request->as<const Coordination::KVMultiDeleteRequest>();

            /// Do nothing if not column family
            auto cf_handler = tryGetColumnFamilyHandler(req->column_family);
            if (!cf_handler)
                break;

            for (const auto & key : req->keys)
            {
                auto status = rocksdb_ptr->Delete(rocksdb::WriteOptions(), cf_handler, key);
                if (!status.ok())
                {
                    /// NOTICE: There is incomplete rollback operation in nuraft, so it will cause the system to exit.
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB delete '{}' error: {}", key, status.ToString());
                }
            }
            break;
        }
        case Coordination::KVOpNum::LIST:
        {
            auto req = request->as<const Coordination::KVListRequest>();
            auto resp = response->as<Coordination::KVListResponse>();
            rangeGetByPrefix(req->key_prefix, &(resp->kv_pairs), req->column_family);
            break;
        }
        default:
            break;
    }

    auto status = writeLogIndex(log_idx);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write log index error: {}", status.ToString());

    last_committed_idx = log_idx;
    return getBufferFromKVResponse(response);
}

bool MetaStateMachine::apply_snapshot(nuraft::snapshot & s)
{
    LOG_DEBUG(log, "Applying snapshot {}", s.get_last_log_idx());
    nuraft::ptr<nuraft::buffer> latest_snapshot_ptr;
    {
        std::lock_guard lock(snapshots_lock);
        uint64_t latest_idx = latest_snapshot_meta->snapshot_meta->get_last_log_idx();
        if (s.get_last_log_idx() != latest_idx)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Required to apply snapshot with last log index {}, but our last log index is {}",
                s.get_last_log_idx(),
                latest_idx);
        latest_snapshot_ptr = latest_snapshot_buf;
    }

    {
        std::lock_guard lock(storage_lock);
        this->shutdownStorage();
        /// note: we move `rocksdb_ptr.reset()` to `shutdownStorage`
        snapshot_manager.restoreFromSnapshot(rocksdb_dir, s.get_last_log_idx());
        initDB();
    }
    last_committed_idx = s.get_last_log_idx();
    return true;
}

nuraft::ptr<nuraft::snapshot> MetaStateMachine::last_snapshot()
{
    /// Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshots_lock);
    return latest_snapshot_meta ? latest_snapshot_meta->snapshot_meta : nullptr;
}

void MetaStateMachine::create_snapshot(nuraft::snapshot & s, nuraft::async_result<bool>::handler_type & when_done)
{
    LOG_DEBUG(log, "Creating snapshot {}", s.get_last_log_idx());

    nuraft::ptr<nuraft::buffer> snp_buf = s.serialize();
    auto snapshot_meta_copy = nuraft::snapshot::deserialize(*snp_buf);
    CreateMetaSnapshotTask snapshot_task;
    {
        std::lock_guard lock(storage_lock);
        snapshot_task.snapshot = std::make_shared<MetaSnapshot>(snapshot_meta_copy);
    }

    snapshot_task.create_snapshot = [this, when_done](MetaSnapshotPtr && snapshot) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        nuraft::ptr<std::exception> exception(nullptr);
        bool ret = true;
        try
        {
            {
                std::lock_guard lock(snapshots_lock);
                latest_snapshot_buf = snapshot_manager.serializeSnapshotBufferToDisk(rocksdb_ptr.get(), *snapshot);
                latest_snapshot_meta = snapshot;

                LOG_DEBUG(log, "Created persistent snapshot {} ", latest_snapshot_meta->snapshot_meta->get_last_log_idx());
            }
        }
        catch (...)
        {
            LOG_TRACE(log, "Exception happened during snapshot");
            tryLogCurrentException(log);
            ret = false;
        }

        when_done(ret, exception);
    };

    LOG_DEBUG(log, "In memory snapshot {} created, queueing task to flash to disk", s.get_last_log_idx());
    /// Flush snapshot to disk in a separate thread.
    if (!snapshots_queue.push(std::move(snapshot_task)))
        LOG_WARNING(log, "Cannot push snapshot task into queue");
}

void MetaStateMachine::save_logical_snp_obj(
    nuraft::snapshot & s, uint64_t & obj_id, nuraft::buffer & data, bool is_first_obj, bool is_last_obj)
{
    LOG_DEBUG(log, "Saving snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);

    nuraft::ptr<nuraft::buffer> cloned_buffer;

    cloned_buffer = nuraft::buffer::clone(data);

    if (obj_id == 0 && is_first_obj)
    {
        std::lock_guard lock(storage_lock);

        // TODO: use another temporary variable to store MetaSnapshot before the transfer of backup
        //  files complete, to avoid use invalid on-going snapshot
        latest_snapshot_buf = cloned_buffer;
        latest_snapshot_meta = snapshot_manager.deserializeSnapshotFromBuffer(cloned_buffer);
    }

    try
    {
        std::lock_guard lock(snapshots_lock);
        snapshot_manager.saveBackupFileOfSnapshot(*latest_snapshot_meta, obj_id, *cloned_buffer);
        LOG_DEBUG(log, "Saved #{} of snapshot {} to path {}", obj_id, s.get_last_log_idx(), latest_snapshot_meta->files[obj_id]);
        obj_id++;

        if (is_last_obj)
        {
            snapshot_manager.finalizeSnapshot(s.get_last_log_idx());
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

int MetaStateMachine::read_logical_snp_obj(
    nuraft::snapshot & s, void *& /*user_snp_ctx*/, uint64_t obj_id, nuraft::ptr<nuraft::buffer> & data_out, bool & is_last_obj)
{
    LOG_DEBUG(log, "Reading snapshot {} obj_id {}", s.get_last_log_idx(), obj_id);
    if (obj_id == 0)
    {
        data_out = nuraft::buffer::clone(*latest_snapshot_buf);
        is_last_obj = false;
    }
    else
    {
        std::lock_guard lock(snapshots_lock);
        if (s.get_last_log_idx() != latest_snapshot_meta->snapshot_meta->get_last_log_idx())
        {
            LOG_WARNING(
                log,
                "Required to apply snapshot with last log index {}, but our last log index is {}. Will ignore this one and retry",
                s.get_last_log_idx(),
                latest_snapshot_meta->snapshot_meta->get_last_log_idx());
            return -1;
        }
        data_out = snapshot_manager.loadBackupFileOfSnapshot(*latest_snapshot_meta, obj_id);
        is_last_obj = (obj_id == latest_snapshot_meta->files.size() - 1);
    }
    return 1;
}

void MetaStateMachine::getByKey(const std::string & key, std::string * value, const std::string & column_family) const
{
    auto status = rocksdb_ptr->Get(rocksdb::ReadOptions(), getColumnFamilyHandler(column_family), key, value);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB get '{}' error: {}", key, status.ToString());
}

void MetaStateMachine::multiGetByKeys(
    const std::vector<std::string> & keys, std::vector<std::string> * values, const std::string & column_family) const
{
    auto cf = getColumnFamilyHandler(column_family);
    auto statuses = rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), {keys.size(), cf}, convertStringsToSlices(keys), values);
    for (size_t i = 0; i < statuses.size(); ++i)
        if (!statuses[i].ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB get '{}' error: {}", keys[i], statuses[i].ToString());
}

void MetaStateMachine::rangeGetByPrefix(
    const std::string & prefix, Coordination::KVStringPairs * kv_pairs, const std::string & column_family) const
{
    /// Return empty if columns familiy not exists.
    auto cf = tryGetColumnFamilyHandler(column_family);
    if (!cf)
        return;

    rocksdb::ReadOptions read_options{};
    read_options.auto_prefix_mode = true;

    std::unique_ptr<rocksdb::Iterator> iter;
    if (prefix.empty())
    {
        iter.reset(rocksdb_ptr->NewIterator(read_options, cf));
        iter->SeekToFirst();
    }
    else
    {
        read_options.prefix_same_as_start = true;
        iter.reset(rocksdb_ptr->NewIterator(read_options, cf));
        iter->Seek(prefix);
    }

    for (; iter->Valid(); iter->Next())
        kv_pairs->emplace_back(iter->key().ToString(), iter->value().ToString());
}

void MetaStateMachine::shutdownStorage()
{
    if (!rocksdb_ptr)
        return;

    for (auto iter = column_families.begin(); iter != column_families.end(); ++iter)
    {
        if (iter->second != nullptr)
        {
            auto status = rocksdb_ptr->DestroyColumnFamilyHandle(iter->second);
            if (!status.ok())
                LOG_ERROR(log, "Failed to destroy column family handle");
        }
    }
    system_cf_handler = nullptr;
    column_families.clear();

    auto status = rocksdb_ptr->Close();
    if (!status.ok())
        LOG_ERROR(log, "Failed to close rocksdb");

    rocksdb_ptr.reset();
}

rocksdb::ColumnFamilyHandle * MetaStateMachine::tryGetColumnFamilyHandler(const std::string & column_family) const
{
    std::shared_lock lock(cf_mutex);
    auto iter = column_families.find(column_family);
    if (iter == column_families.end())
        return nullptr;

    return iter->second;
}

rocksdb::ColumnFamilyHandle * MetaStateMachine::getColumnFamilyHandler(const std::string & column_family) const
{
    auto handle = tryGetColumnFamilyHandler(column_family);
    if (!handle)
        throw Exception("Fail to get column families '" + column_family + "'", ErrorCodes::STD_EXCEPTION);

    return handle;
}

rocksdb::ColumnFamilyHandle * MetaStateMachine::getOrCreateColumnFamilyHandler(const std::string & column_family)
{
    if (auto * handle = tryGetColumnFamilyHandler(column_family))
        return handle;

    std::lock_guard lock(cf_mutex);

    /// Double check again
    auto iter = column_families.find(column_family);
    if (iter != column_families.end())
        return iter->second;

    /// Create new column family
    assert(column_families.size() > 0);
    rocksdb::ColumnFamilyDescriptor cf_descriptor;
    rocksdb::ColumnFamilyHandle * handle;

    auto status = column_families.begin()->second->GetDescriptor(&cf_descriptor);
    if (status != rocksdb::Status::OK())
        throw Exception(
            "Fail to get rocksdb column families descriptor at " + column_families.begin()->second->GetName() + ": "
                + status.ToString(),
            ErrorCodes::ROCKSDB_ERROR);

    status = rocksdb_ptr->CreateColumnFamily(cf_descriptor.options, column_family, &handle);
    if (status != rocksdb::Status::OK())
        throw Exception("Fail to create rocksdb column families '" + column_family + "' : " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);

    column_families[column_family] = handle;
    return handle;
}

void MetaStateMachine::initDB()
{
    //// FIXME: Option Configurable
    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.compression = rocksdb::CompressionType::kZSTD;

    /// Enable prefix seek
    options.prefix_extractor.reset(new Coordination::NamespacePrefixTransform());

    /// Enable prefix bloom for mem tables, size: `write_buffer_size(default: 64MB) * memtable_prefix_bloom_size_ratio`
    options.memtable_whole_key_filtering = true;
    options.memtable_prefix_bloom_size_ratio = 0.1;

    /// Enable prefix bloom for SST file
    rocksdb::BlockBasedTableOptions table_options;
    table_options.whole_key_filtering = true;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10 /* bits_per_key */, true));
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    /// Init column families
    rocksdb::DBOptions db_options(options);
    rocksdb::ColumnFamilyOptions cf_options(options);
    std::vector<std::string> cf_names;
    auto status = rocksdb::DB::ListColumnFamilies(db_options, rocksdb_dir, &cf_names);
    if (status != rocksdb::Status::OK())
    {
        LOG_WARNING(log, "Fail to get rocksdb column families list at: {} : {} ", rocksdb_dir, status.ToString());
        cf_names.clear();
    }

    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
    cf_descriptors.emplace_back(_TP_SYSTEM_COLUMN_FAMILY_NAME, cf_options);  /// set default system column family
    for (auto & name : cf_names)
    {
        if (name == _TP_SYSTEM_COLUMN_FAMILY_NAME)
            continue;

        cf_descriptors.emplace_back(name, cf_options);
    }

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handlers;

    /// Open Rocksdb with column families
    status = rocksdb::DB::Open(options, rocksdb_dir, cf_descriptors, &cf_handlers, &db);
    if (status != rocksdb::Status::OK())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to open rocksdb path at: {}: {}", rocksdb_dir, status.ToString());

    rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);

    assert(cf_descriptors.size() == cf_handlers.size());

    std::lock_guard lock(cf_mutex);
    for (size_t i = 0; i < cf_descriptors.size(); ++i)
    {
        column_families.emplace(cf_descriptors[i].name, cf_handlers[i]);

        if (cf_descriptors[i].name == _TP_SYSTEM_COLUMN_FAMILY_NAME)
            system_cf_handler = cf_handlers[i];  // cache system cf reference
    }

    assert(system_cf_handler);
}

rocksdb::Status MetaStateMachine::writeLogIndex(uint64_t log_idx)
{
    rocksdb::WriteBatch batch;
    rocksdb::Slice idx_value{reinterpret_cast<char*>(&log_idx), sizeof(log_idx)};
    auto status = batch.Put(system_cf_handler, META_LOG_INDEX_KEY, idx_value);
    if (!status.ok())
        return status;

    return rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
}

}
