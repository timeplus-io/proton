#pragma once

#include <Checkpoint/CheckpointRequest.h>
#include <Processors/ISource.h>
#include <Common/serde.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{
class ISource : public DB::ISource
{
public:
    ISource(Block header, bool enable_auto_progress, ProcessorID pid_) : DB::ISource(std::move(header), enable_auto_progress, pid_)
    {
        is_streaming = true;
    }

    virtual String description() const { return ""; }

    /// \brief Get the last progressed sequence number of the source, it shouldn't be called during pipeline execution (thread-unsafe)
    virtual Int64 lastSN() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for lastSN() of {}", getName()); }

    /// \brief Reset the start sequence number of the source, it must be called before the pipeline execution (thread-unsafe)
    void resetStartSN(Int64 sn);

    Int64 lastCheckpointedSN() const noexcept { return last_checkpointed_sn; }

    void checkpoint(CheckpointContextPtr ckpt_ctx_) override final;
    void recover(CheckpointContextPtr ckpt_ctx_) override final;

private:
    std::optional<Chunk> tryGenerate() override final;

    /// \brief Checkpointing the source state (include lastSN())
    virtual Chunk doCheckpoint(CheckpointContextPtr)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for checkpoting of {}", getName());
    }

    /// \brief Recovering the source state (include lastSN())
    virtual void doRecover(CheckpointContextPtr)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for recovering of {}", getName());
    }

    /// \brief Reset current consume sequence number, it must be called before the pipeline execution (thread-unsafe)
    virtual void doResetStartSN(Int64 /*sn*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for doResetStartSN() of {}", getName());
    }

private:
    /// For checkpoint
    CheckpointRequest ckpt_request;
    NO_SERDE std::optional<Int64> reseted_start_sn;
    NO_SERDE Int64 last_checkpointed_sn = -1;
};
}
}
