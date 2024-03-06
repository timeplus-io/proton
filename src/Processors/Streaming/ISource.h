#pragma once

#include <Checkpoint/CheckpointRequest.h>
#include <Processors/ISource.h>

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

    /// \brief Get the last progressed sequence number of the source, it shouldn't be called during pipeline execution (thread-unsafe)
    virtual Int64 lastSN() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for lastSN of {}", getName()); }

    /// \brief Reset the sequence number of the source, it should be called before the pipeline execution (thread-unsafe)
    virtual void resetSN(Int64 /*sn*/) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for resetSN of {}", getName()); }

    void checkpoint(CheckpointContextPtr ckpt_ctx_) override final;

private:
    std::optional<Chunk> tryGenerate() override final;

    /// \brief Checkpointing the source state
    virtual Chunk doCheckpoint(CheckpointContextPtr)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for checkpoting of {}", getName());
    }

private:
    /// For checkpoint
    CheckpointRequest ckpt_request;
};
}
}
