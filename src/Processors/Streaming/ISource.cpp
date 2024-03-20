#include <Processors/Streaming/ISource.h>

namespace DB::Streaming
{
/// It basically initiate a checkpoint
/// Since the checkpoint method is called in a different thread (CheckpointCoordinator)
/// We need make sure it is thread safe
void ISource::checkpoint(CheckpointContextPtr ckpt_ctx_)
{
    /// We assume the previous ckpt is already done
    ckpt_request.setCheckpointRequestCtx(std::move(ckpt_ctx_));
}

void ISource::recover(CheckpointContextPtr ckpt_ctx_)
{
    doRecover(std::move(ckpt_ctx_));
    last_checkpointed_sn = lastProcessedSN();

    /// Reset consume offset started from the next of last checkpointed sn (if not manually reset before recovery)
    if (!reseted_start_sn.has_value())
        doResetStartSN(last_checkpointed_sn + 1);
}

void ISource::resetStartSN(Int64 sn)
{
    reseted_start_sn = sn;
    doResetStartSN(sn);
}

std::optional<Chunk> ISource::tryGenerate()
{
    if (auto current_ckpt_ctx = ckpt_request.poll(); current_ckpt_ctx)
    {
        auto chunk = doCheckpoint(std::move(current_ckpt_ctx));
        last_checkpointed_sn = lastProcessedSN();
        return std::move(chunk);
    }

    auto chunk = generate();
    if (!chunk)
        return {};

    return chunk;
}
}
