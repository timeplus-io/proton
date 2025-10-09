#pragma once

#include <Checkpoint/Checkpoint.h>
#include <Checkpoint/CheckpointContextFwd.h>
#include <Checkpoint/CheckpointStorageType.h>
#include <Checkpoint/DiskPath.h>

#include <Core/PathSize.h>
#include <Core/Types.h>
#include <base/types.h>

#include <filesystem>
#include <set>
#include <utility>

namespace DB
{
class WriteBuffer;
class ReadBuffer;

/// All checkpoint storage must be stateless, and the state is managed by CheckpointContext
class CheckpointStorage
{
public:
    explicit CheckpointStorage(const std::string & log_name) : logger(::getLogger(log_name)) { }
    virtual ~CheckpointStorage() = default;

    LoggerPtr getLogger() const { return logger; }

    virtual void preCheckpoint(CheckpointContextPtr) const { }

    virtual void checkpoint(const std::string & key, CheckpointPtr ckpt, CheckpointContextPtr ckpt_ctx) const = 0;

    virtual CheckpointPtr recover(const std::string & key, CheckpointContextPtr ckpt_ctx) const = 0;

    virtual void commit(CheckpointContextPtr ckpt_ctx) const = 0;

    /// Get and set the last checkpoint epoch via query id
    virtual int64_t getLastCommittedEpoch(CheckpointContextPtr ckpt_ctx) const = 0;

    /// Get all key names of the checkpoint
    virtual std::set<std::string> getKeyNames(CheckpointContextPtr ckpt_ctx) const = 0;

    /// Remove all ckpts before ckpt_ctx->epic
    /// If ckpt_ctx->epic is zero, remove the whole ckpt folder for the query
    virtual void remove(CheckpointContextPtr ckpt_ctx) const = 0;

    /// Loop all checkpoints to remove expired checkpoints or checkpoints which have a remove mark
    virtual void removeExpired(uint64_t ttl_secs, bool delete_marked, std::function<bool(const std::string &)> delete_precheck) const = 0;

    /// Loop all checkpoints to remove old checkpoints (i.e. checkpoints which are less than last committed epoch)
    virtual void removeOldCheckpoints() const = 0;

    /// \return checkpoint type if exists, otherwise std::nullopt
    virtual std::optional<CheckpointType> exists(const std::string & key, CheckpointContextPtr cpt_ctx) const = 0;

    virtual CheckpointStorageType storageType() const = 0;

    virtual uint64_t getStorageSize(CheckpointContextPtr ckpt_ctx) const = 0;
    virtual PathSizes getStorageStat(CheckpointContextPtr ckpt_ctx) const = 0;

    virtual bool isLocal() const { return false; }

protected:
    LoggerPtr logger;
};
}
