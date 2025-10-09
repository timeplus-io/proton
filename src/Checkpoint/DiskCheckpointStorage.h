#pragma once

#include <Checkpoint/CheckpointStorage.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

namespace DB
{
/// Checkpoint on disk layout
/// /var/lib/proton/checkpoint <-- base_dir (i.e. disk root path)
///     |-- /{query_id}/
///     |        |-- query.ckpt
///     |        |-- dag.ckpt
///     |        |-- settings.ckpt
///     |        |-- <epoch1>
///     |        |      |-- committed.ckpt
///     |        |      |-- <processor-1.ckpt>
///     |        |      |-- <processor-2.ckpt>
///     |        |      |-- <processor-3.ckpt>
///     |        |      |-- ...
///     |        |
///     |        |-- <epoch2>
///     |               |-- committed.ckpt
///     |               |-- <processor-1.ckpt>
///     |               |-- <processor-2.ckpt>
///     |               |-- <processor-3.ckpt>
///     |               |-- ...
///     |-- /{query_id}/
///     | ...
//// For a specific query, when epoch progress, we can delete checkpoints in old epochs
class DiskCheckpointStorage : public CheckpointStorage
{
public:
    DiskCheckpointStorage(const std::string & log_name);

    virtual DiskPtr getDisk(CheckpointContextPtr ckpt_ctx) const = 0;

    void preCheckpoint(CheckpointContextPtr ckpt_ctx) const override;

    void checkpoint(const std::string & key, CheckpointPtr ckpt, CheckpointContextPtr ckpt_ctx) const override;

    CheckpointPtr recover(const std::string & key, CheckpointContextPtr ckpt_ctx) const override;

    void commit(CheckpointContextPtr ckpt_ctx) const override;

    /// Get the last checkpoint epoch for a query id
    int64_t getLastCommittedEpoch(CheckpointContextPtr ckpt_ctx) const override;

    std::set<std::string> getKeyNames(CheckpointContextPtr ckpt_ctx) const override;

    std::unique_ptr<WriteBufferFromFileBase> initKeyFileWriteBuffer(const std::string & key, CheckpointContextPtr ckpt_ctx) const;
    std::unique_ptr<ReadBufferFromFileBase> initKeyFileReadBuffer(const std::string & key, CheckpointContextPtr ckpt_ctx) const;

    void remove(CheckpointContextPtr ckpt_ctx) const override;

    void removeExpired(uint64_t ttl_secs, bool delete_marked, std::function<bool(const std::string &)> delete_precheck) const override;

    void removeOldCheckpoints() const override;

    std::optional<CheckpointType> exists(const std::string & key, CheckpointContextPtr ckpt_ctx) const override;

    uint64_t getStorageSize(CheckpointContextPtr ckpt_ctx) const override;
    PathSizes getStorageStat(CheckpointContextPtr ckpt_ctx) const override;

    bool checkpointDirExists(CheckpointContextPtr ckpt_ctx) const;
};
}
