#include <Coordination/MetaStateMachine.h>

#include <Coordination/KVRequest.h>
#include <Coordination/KVResponse.h>
#include <Coordination/MetaSnapshotManager.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>

#include <future>

namespace DB
{
[[maybe_unused]] std::string META_LOG_INDEX_KEY = "metastore_log_index";

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
    LOG_DEBUG(log, "Totally have {} snapshots", snapshot_manager.totalSnapshots());

    //    bool has_snapshots = snapshot_manager.totalSnapshots() != 0;
    std::string result;
    rocksdb::Status status = rocksdb_ptr->Get(rocksdb::ReadOptions(), META_LOG_INDEX_KEY, &result);
    if (status.ok())
    {
        //        throw Exception("RocksDB load error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
        last_committed_idx = std::stoi(result);
    }
    LOG_DEBUG(log, "last committed log index {}", last_committed_idx);
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
            LOG_WARNING(
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
    auto response = Coordination::KVResponseFactory::instance().get(op);

    if (op == Coordination::KVOpNum::MULTIPUT || op == Coordination::KVOpNum::PUT)
    {
        auto write_batch = [this](uint64_t idx, const std::vector<std::string> & keys, const std::vector<std::string> & values) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
            rocksdb::Status status;
            rocksdb::WriteBatch batch;
            auto count = std::min(keys.size(), values.size());
            for (size_t i = 0; i < count; ++i)
            {
                status = batch.Put(keys[i], values[i]);
                if (!status.ok())
                    return status;
            }

            status = batch.Put(META_LOG_INDEX_KEY, std::to_string(idx));
            if (!status.ok())
                return status;

            status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
            return status;
        };

        rocksdb::Status status;
        if (op == Coordination::KVOpNum::MULTIPUT)
        {
            auto req = request->as<const Coordination::KVMultiPutRequest>();
            status = write_batch(log_idx, req->keys, req->values);
        }
        else
        {
            auto req = request->as<const Coordination::KVPutRequest>();
            status = write_batch(log_idx, {req->key}, {req->value});
        }
        if (!status.ok())
        {
            /// NOTICE: There is incomplete rollback operation in nuraft, so it will cause the system to exit.
            throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
        }
    }
    else if (op == Coordination::KVOpNum::MULTIGET)
    {
        auto req = request->as<const Coordination::KVMultiGetRequest>();
        auto resp = response->as<Coordination::KVMultiGetResponse>();
        const auto & slices_keys = convertStringsToSlices(req->keys);
        auto statuses = rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), slices_keys, &(resp->values));
        for (auto status : statuses)
            if (!status.ok())
            {
                /// NOTICE: There is incomplete rollback operation in nuraft, so it will cause the system to exit.
                // throw Exception("RocksDB read error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);

                /// To avoid to exit for exception, even if the read failed, we also consider this commit successful,
                /// and return error status.
                resp->code = ErrorCodes::ROCKSDB_ERROR;
                resp->msg = "RocksDB read error: " + status.ToString();
                break;
            }
    }
    else if (op == Coordination::KVOpNum::GET)
    {
        auto req = request->as<const Coordination::KVGetRequest>();
        auto resp = response->as<Coordination::KVGetResponse>();
        auto status = rocksdb_ptr->Get(rocksdb::ReadOptions(), req->key, &(resp->value));
        if (!status.ok())
        {
            /// NOTICE: There is incomplete rollback operation in nuraft, so it will cause the system to exit.
            // throw Exception("RocksDB read error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);

            /// To avoid to exit for exception, even if the read failed, we also consider this commit successful,
            /// and return error status.
            resp->code = ErrorCodes::ROCKSDB_ERROR;
            resp->msg = "RocksDB read error: " + status.ToString();
        }
    }
    else if (op == Coordination::KVOpNum::MULTIDELETE)
    {
        auto req = request->as<const Coordination::KVMultiDeleteRequest>();
        for (const auto & key : req->keys)
        {
            auto status = rocksdb_ptr->Delete(rocksdb::WriteOptions(), key);
            if (!status.ok())
            {
                /// NOTICE: There is incomplete rollback operation in nuraft, so it will cause the system to exit.
                throw Exception("RocksDB delete error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
            }

        }
    }
    else if (op == Coordination::KVOpNum::DELETE)
    {
        auto req = request->as<const Coordination::KVDeleteRequest>();
        auto status = rocksdb_ptr->Delete(rocksdb::WriteOptions(), req->key);
        if (!status.ok())
        {
            /// NOTICE: There is incomplete rollback operation in nuraft, so it will cause the system to exit.
            throw Exception("RocksDB delete error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
        }
    }

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
        rocksdb_ptr.reset();
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
    snapshots_queue.push(std::move(snapshot_task));
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

void MetaStateMachine::getByKey(const std::string & key, std::string * value) const
{
    auto status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key, value);
    if (!status.ok())
        throw Exception("RocksDB read error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
}

void MetaStateMachine::multiGetByKeys(const std::vector<std::string> & keys, std::vector<std::string> * values) const
{
    auto statuses = rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), convertStringsToSlices(keys), values);
    for (auto status : statuses)
        if (!status.ok())
            throw Exception("RocksDB read error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
}

void MetaStateMachine::shutdownStorage()
{
    //    std::lock_guard lock(storage_lock);
    //    storage->finalize();
}

void MetaStateMachine::initDB()
{
    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    options.compression = rocksdb::CompressionType::kZSTD;
    rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_dir, &db);

    if (status != rocksdb::Status::OK())
        throw Exception("Fail to open rocksdb path at: " + rocksdb_dir + ": " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
    rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
}

}
