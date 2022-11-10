#include "CheckpointStorage.h"

namespace DB
{
/// Checkpoint filesystem layout
/// /var/lib/proton/checkpoint <-- base_dir
///     |-- /{query_id}/
///     |        |-- meta.ckpt
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
//// For a specific query, when epic progress, we can delete checkpoints in old epics
class LocalFileSystemCheckpoint final : public CheckpointStorage
{
public:
    LocalFileSystemCheckpoint(const std::string & base_dir_, uint16_t interval_);

    void checkpoint(
        VersionType version, const std::string & key, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt) override;

    void commit(CheckpointContextPtr ckpt_ctx) override;

    /// Recover the last checkpoint epoch for a query id and clean up any uncommitted ckpt etc
    int64_t recover(const std::string & qid) override;

    void recover(
        const std::string & key, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType version, ReadBuffer &)> do_recover) override;

    void markRemove(CheckpointContextPtr ckpt_ctx) override;

    void remove(CheckpointContextPtr ckpt_ctx) override;

    void removeExpired(uint64_t ttl_secs, bool delete_marked, std::function<bool(const std::string &)> delete_precheck) override;

    bool exists(const std::string & key, CheckpointContextPtr ckpt_ctx) const override;

    const std::filesystem::path & baseDirectory() const override { return base_dir; }

    uint64_t defaultInterval() const override { return interval; }

    CheckpointStorageType storageType() const override { return CheckpointStorageType::LocalFileSystem; }

    void preCheckpoint(CheckpointContextPtr ckpt_ctx) override;

private:
    std::filesystem::path base_dir;
    uint64_t interval;
};
}
