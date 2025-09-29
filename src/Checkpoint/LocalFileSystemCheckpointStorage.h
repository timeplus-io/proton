#pragma once

#include <Checkpoint/DiskCheckpointStorage.h>

#include <Disks/DiskLocal.h>

namespace DB
{
class LocalFileSystemCheckpointStorage final : public DiskCheckpointStorage
{
public:
    explicit LocalFileSystemCheckpointStorage(const std::string & base_dir_) : DiskCheckpointStorage("LocalFileSystemCheckpointStorage")
    {
        local_disk = std::make_shared<DiskLocal>("local_fs_ckpt_storage", base_dir_);
        if (!local_disk->exists("")) /// If base_dir doesn't exist, create it
            local_disk->createDirectories("");
    }

    CheckpointReplicationType replicationType() const override { return CheckpointReplicationType::LocalFileSystem; }

    bool isLocal() const override { return true; }

    DiskPtr getDisk(CheckpointContextPtr) const override { return local_disk; }

    void preCommit(CheckpointContextPtr ckpt_ctx) const;

    std::optional<UUID> tryGetPreCommittedUUID(CheckpointContextPtr ckpt_ctx) const;

    bool preCommitted(CheckpointContextPtr ckpt_ctx) const;

private:
    DiskPtr local_disk;
};
}
