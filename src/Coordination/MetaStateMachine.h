#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/MetaSnapshotManager.h>
#include <Common/logger_useful.h>

#include <libnuraft/nuraft.hxx>
#include <rocksdb/db.h>

#include <memory>

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

    void getByKey(const std::string & key, std::string * value, const std::string & column_family = {}) const;

    void multiGetByKeys(const std::vector<std::string> & keys, std::vector<std::string> * values, const std::string & column_family = {}) const;

    void rangeGetByPrefix(const std::string & prefix, std::vector<std::pair<std::string, std::string>> * kv_pairs, const std::string & column_family = {}) const;

    rocksdb::ColumnFamilyHandle * tryGetColumnFamilyHandler(const std::string & column_family) const;
    rocksdb::ColumnFamilyHandle * getColumnFamilyHandler(const std::string & column_family) const;
    rocksdb::ColumnFamilyHandle * getOrCreateColumnFamilyHandler(const std::string & column_family);

    void shutdownStorage();

private:
    // RocksDB related variables
    const String primary_key;
    using RocksDBPtr = std::unique_ptr<rocksdb::DB>;
    RocksDBPtr rocksdb_ptr;
    String rocksdb_dir;

    /// Ref the system column family handler to avoid frequent find from column_families
    rocksdb::ColumnFamilyHandle * system_cf_handler;
    std::unordered_map<std::string, rocksdb::ColumnFamilyHandle *> column_families;
    mutable std::shared_mutex cf_mutex;

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
    rocksdb::Status writeLogIndex(uint64_t log_idx);
};

}
