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

    /// \brief Get/Set the last progressed sequence number of the source
    Int64 lastProcessedSN() const noexcept { return last_processed_sn.load(std::memory_order_relaxed); }
    void setLastProcessedSN(Int64 sn) noexcept { last_processed_sn.store(sn, std::memory_order_relaxed); }

    /// \brief Reset the start sequence number of the source, it must be called before the pipeline execution (thread-unsafe)
    void resetStartSN(Int64 sn);

    /// \brief Get the last checkpoint sequence number of the source
    Int64 lastCheckpointSN() const noexcept { return last_ckpt_sn.load(std::memory_order_relaxed); }

    void checkpoint(CheckpointContextPtr ckpt_ctx_) override final;
    void recover(CheckpointContextPtr ckpt_ctx_) override final;

private:
    void setLastCheckpointSN(Int64 ckpt_sn) noexcept { last_ckpt_sn.store(ckpt_sn, std::memory_order_relaxed); }

    std::optional<Chunk> tryGenerate() override final;

    /// \brief Checkpointing the source state (include lastProcessedSN())
    virtual Chunk doCheckpoint(CheckpointContextPtr)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for checkpoting of {}", getName());
    }

    /// \brief Recovering the source state (include lastProcessedSN())
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
    NO_SERDE std::optional<Int64> reset_start_sn;
    NO_SERDE std::atomic<Int64> last_ckpt_sn = -1;
    SERDE std::atomic<Int64> last_processed_sn = -1;
};

using StreamingSourcePtr = std::shared_ptr<ISource>;
using StreamingSourcePtrs = std::vector<StreamingSourcePtr>;
}
}
