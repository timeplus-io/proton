#pragma once

#include "CheckpointContextFwd.h"

#include <base/types.h>

#include <Poco/Logger.h>

#include <filesystem>

namespace DB
{
class WriteBuffer;
class ReadBuffer;

enum class CheckpointStorageType : uint16_t
{
    LocalFileSystem = 1,
};

class CheckpointStorage
{
public:
    explicit CheckpointStorage(const std::string & log_name) : logger(&Poco::Logger::get(log_name)) { }
    virtual ~CheckpointStorage() = default;

    virtual void preCheckpoint(CheckpointContextPtr )  { }

    virtual void
    checkpoint(VersionType version, const std::string & key, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt)
        = 0;

    virtual void commit(CheckpointContextPtr ckpt_ctx) = 0;

    /// Recover the last checkpoint epoch via query id and clean up any uncommitted ckpt etc
    virtual int64_t recover(const std::string & qid) = 0;

    /// Recover checkpoint
    virtual void recover(const std::string & key, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType version, ReadBuffer &)> do_recover) = 0;

    /// @return marked
    virtual bool markRemove(CheckpointContextPtr ckpt_ctx) = 0;

    /// Remove all ckpts before ckpt_ctx->epic
    /// If ckpt_ctx->epic is zero, remove the whole ckpt folder for the query
    virtual void remove(CheckpointContextPtr ckpt_ctx) = 0;

    /// Loop all checkpoints to remove expired checkpoints or checkpoints which have a remove mark
    virtual void removeExpired(uint64_t ttl_secs, bool delete_marked, std::function<bool(const std::string &)> delete_precheck) = 0;

    virtual bool exists(const std::string & key, CheckpointContextPtr cpt_ctx) const = 0;

    virtual CheckpointStorageType storageType() const = 0;

    bool isLocal() const
    {
        switch (storageType())
        {
            case CheckpointStorageType::LocalFileSystem:
                return true;
        }

        return false;
    }

    virtual const std::filesystem::path & baseDirectory() const = 0;
    virtual uint64_t defaultInterval() const = 0;

protected:
    Poco::Logger * logger;
};
}
