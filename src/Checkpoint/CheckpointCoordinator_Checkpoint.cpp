#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#include <chrono>
#include <thread>

namespace DB
{

namespace ErrorCodes
{
extern const int RECOVER_CHECKPOINT_FAILED;
}

void CheckpointCoordinator::preCheckpoint(DB::CheckpointContextPtr ckpt_ctx)
{
    /// Always use local storage directly
    ckpt_ctx->storage.preCheckpoint(ckpt_ctx);
}

void CheckpointCoordinator::checkpoint(
    VersionType version, const String & key, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt)
{
    auto ckpt = std::make_shared<FileCheckpoint>(version, std::move(do_ckpt));
    checkpoint(key, std::move(ckpt), ckpt_ctx);
}

void CheckpointCoordinator::checkpoint(
    VersionType version, UInt32 node_id, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt)
{
    auto ckpt = std::make_shared<FileCheckpoint>(version, std::move(do_ckpt));
    checkpoint(fmt::format("{}", node_id), std::move(ckpt), ckpt_ctx);

    checkpointed(version, node_id, std::move(ckpt_ctx));
}

void CheckpointCoordinator::checkpoint(UInt32 node_id, CheckpointPtr ckpt, CheckpointContextPtr ckpt_ctx)
{
    checkpoint(fmt::format("{}", node_id), ckpt, ckpt_ctx);

    checkpointed(ckpt->getVersion(), node_id, std::move(ckpt_ctx));
}

void CheckpointCoordinator::checkpoint(const String & key, CheckpointPtr ckpt, CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->storage.checkpoint(key, std::move(ckpt), ckpt_ctx);
}

void CheckpointCoordinator::checkpointed(VersionType /*version*/, UInt32 node_id, CheckpointContextPtr ckpt_ctx)
{
    if (node_id == std::numeric_limits<UInt32>::max())
        return; /// Skip non-logical nodes

    bool ckpt_epoch_done = false;
    {
        std::scoped_lock lock(mutex);
        auto iter = queries.find(ckpt_ctx->qid);
        if (iter == queries.end())
            /// Unregistered
            return;

        chassert(ckpt_ctx->epoch == iter->second->current_epoch);
        chassert(ckpt_ctx->request_ctx);

        if (iter->second->ack(node_id))
        {
            /// All ack nodes in the DAG have acked
            /// 1) Commit `committed` flag file to query ckpt directory for that epoch
            ckpt_epoch_done = commit(ckpt_ctx);

            /// 2) Update committed epoch metrics
            ckpt_ctx->request_ctx->metrics->stopwatch.stop();
            iter->second->metrics = ckpt_ctx->request_ctx->metrics;
        }
    }

    if (ckpt_epoch_done)
        ckpt_ctx->request_ctx->onFinish(ckpt_ctx);
}

bool CheckpointCoordinator::commit(CheckpointContextPtr ckpt_ctx)
{
    /// Direct commit to local storage
    chassert(ckpt_ctx->request_ctx);
    ckpt_ctx->storage.commit(ckpt_ctx);
    return commitCurrentCheckpointEpochWithLock(ckpt_ctx);
}

void CheckpointCoordinator::recover(
    const String & key, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType version, ReadBuffer &)> do_recover, bool log) const
{
    /// Direct recovery from local storage
    auto ckpt = ckpt_ctx->storage.recover(key, ckpt_ctx);

    if (ckpt->type() != CheckpointType::File)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Dismatched checkpoint type for key={}, expected 'File' but got '{}'",
            key,
            ckpt->type());

    std::static_pointer_cast<FileCheckpoint>(ckpt)->recover([&](ReadBuffer & rb) { do_recover(ckpt->getVersion(), rb); }, log);
}

void CheckpointCoordinator::recover(
    UInt32 node_id, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType version, ReadBuffer &)> do_recover) const
{
    return recover(fmt::format("{}", node_id), std::move(ckpt_ctx), std::move(do_recover));
}

CheckpointPtr CheckpointCoordinator::recover(UInt32 node_id, CheckpointContextPtr ckpt_ctx) const
{
    /// Direct recovery from local storage
    auto key = fmt::format("{}", node_id);
    return ckpt_ctx->storage.recover(key, ckpt_ctx);
}

void CheckpointCoordinator::recoverGraph(CheckpointContextPtr ckpt_ctx, std::function<void(VersionType, ReadBuffer &)> do_recover) const
{
    recover(GRAPH_CKPT_FILE, std::move(ckpt_ctx), std::move(do_recover));
}

bool CheckpointCoordinator::commitCurrentCheckpointEpochWithLock(CheckpointContextPtr ckpt_ctx)
{
    auto iter = queries.find(ckpt_ctx->qid);
    if (iter == queries.end())
    {
        LOG_INFO(logger, "Skip to commit epoch {}. The query has been deregistered", ckpt_ctx->epoch);
        return false;
    }

    if (iter->second->current_epoch != ckpt_ctx->epoch)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Failed to commit epoch={} for query={}, current_epoch={} last_epoch={}",
            ckpt_ctx->epoch,
            ckpt_ctx->qid,
            iter->second->current_epoch,
            iter->second->last_epoch);

    iter->second->prepareForNextEpoch();
    resetHeavyCheckpointingInProgress(ckpt_ctx->qid);
    return true;
}
}
