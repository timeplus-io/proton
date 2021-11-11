#pragma once

#include <memory>

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <common/logger_useful.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/MetaSnapshotManager.h>

#include <rocksdb/db.h>

namespace DB
{

using MetaSnapshotsQueue = ConcurrentBoundedQueue<CreateMetaSnapshotTask>;

class MetaStateMachine : public nuraft::state_machine
{
public:
    MetaStateMachine(
        MetaSnapshotsQueue & snapshots_queue_,
        const std::string & snapshots_path_,
        const std::string & db_path_,
        const CoordinationSettingsPtr & coordination_settings_);

    void init();

    nuraft::ptr<nuraft::buffer> pre_commit(const uint64_t /*log_idx*/, nuraft::buffer & /*data*/) override { return nullptr; }

    nuraft::ptr<nuraft::buffer> commit(const uint64_t log_idx, nuraft::buffer & data) override;

    void rollback(const uint64_t /*log_idx*/, nuraft::buffer & /*data*/) override {}

    uint64_t last_commit_index() override { return last_committed_idx; }

    bool apply_snapshot(nuraft::snapshot & s) override;

    nuraft::ptr<nuraft::snapshot> last_snapshot() override;

    void create_snapshot(
        nuraft::snapshot & s,
        nuraft::async_result<bool>::handler_type & when_done) override;

    void save_logical_snp_obj(
        nuraft::snapshot & s,
        uint64_t & obj_id,
        nuraft::buffer & data,
        bool is_first_obj,
        bool is_last_obj) override;

    int read_logical_snp_obj(
        nuraft::snapshot & s,
        void* & user_snp_ctx,
        uint64_t obj_id,
        nuraft::ptr<nuraft::buffer> & data_out,
        bool & is_last_obj) override;

    void getByKey(const std::string & key, std::string * value) const;

    void multiGetByKeys(const std::vector<std::string> & keys, std::vector<std::string> * values) const;

    void shutdownStorage();

    int64_t getValue() const { return 100; }


private:
    // RocksDB related variables
    const String primary_key;
    using RocksDBPtr = std::unique_ptr<rocksdb::DB>;
    RocksDBPtr rocksdb_ptr;
    String rocksdb_dir;

    MetaSnapshotPtr latest_snapshot_meta = nullptr;
    nuraft::ptr<nuraft::buffer> latest_snapshot_buf = nullptr;

    CoordinationSettingsPtr coordination_settings;

//    KeeperStoragePtr storage;

    MetaSnapshotManager snapshot_manager;

//    ResponsesQueue & responses_queue;

    MetaSnapshotsQueue & snapshots_queue;
    /// Mutex for snapshots
    std::mutex snapshots_lock;

    /// Lock for storages
    std::mutex storage_lock;

    /// Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx;
    Poco::Logger * log;

    void initDB();
};

}
