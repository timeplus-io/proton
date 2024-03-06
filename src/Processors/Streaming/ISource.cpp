#include <Processors/Streaming/ISource.h>

namespace DB::Streaming
{
/// It basically initiate a checkpoint
/// Since the checkpoint method is called in a different thread (CheckpointCoordinator)
/// We nee make sure it is thread safe
void ISource::checkpoint(CheckpointContextPtr ckpt_ctx_)
{
    /// We assume the previous ckpt is already done
    ckpt_request.setCheckpointRequestCtx(std::move(ckpt_ctx_));
}

std::optional<Chunk> ISource::tryGenerate()
{
    if (auto current_ckpt_ctx = ckpt_request.poll(); current_ckpt_ctx)
        return doCheckpoint(std::move(current_ckpt_ctx));

    auto chunk = generate();
    if (!chunk)
        return {};

    return chunk;
}
}
