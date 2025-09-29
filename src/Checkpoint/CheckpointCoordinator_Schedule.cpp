#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>
#include <Checkpoint/LocalFileSystemCheckpointStorage.h>
#include <Checkpoint/LogStoreCheckpointContext.h>

#include <Common/Random.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
extern const int CHECKPOINT_ALREADY_EXISTS;
}

namespace
{
/// Random retry interval between 5~15s
ALWAYS_INLINE UInt64 retryInterval()
{
    return globalRandomInt<UInt64>(5, 15);
}

/// Random interval between [interval*(1-ratio), interval*(1+ratio)]
ALWAYS_INLINE UInt64 randomInterval(UInt64 interval, double ratio)
{
    UInt64 low = std::max<UInt64>(static_cast<UInt64>(interval * (1 - ratio)), 1);
    UInt64 high = static_cast<UInt64>(interval * (1 + ratio));
    return globalRandomInt<UInt64>(low, high);
}
}

void CheckpointCoordinator::triggerCheckpoint(const String & qid) noexcept
{
    UInt64 ckpt_interval = 5;
    try
    {
        CheckpointContextPtr ckpt_ctx;
        {
            std::scoped_lock lock(mutex);

            auto iter = queries.find(qid);
            if (iter == queries.end())
                /// Already canceled the query
                return;

            ckpt_ctx = iter->second->prepareNextCheckpointContext();
            ckpt_interval = iter->second->checkpointInterval(config);
        }

        if (doTriggerCheckpoint(ckpt_ctx) == TriggerResult::Success)
            /// For a heavy checkpoint, prefer to trigger next by a random interval between [interval*0.9, interval*1.1] (such as 900 -> [810, 990])
            triggerCheckpointAfter(isHeavyCheckpoint(ckpt_ctx) ? randomInterval(ckpt_interval, 0.1f) : ckpt_interval, qid);
        else
            /// Retry after interval - min(5~15s, ckpt_interval)
            triggerCheckpointAfter(std::min(retryInterval(), ckpt_interval), qid);
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to trigger checkpoint");

        resetCurrentCheckpointEpoch(qid);
        triggerCheckpointAfter(std::min(retryInterval(), ckpt_interval), qid);
    }
}

void CheckpointCoordinator::triggerCheckpointAfter(uint64_t delay_seconds, const String & qid)
{
    chassert(delay_seconds > 0);
    std::scoped_lock lock(mutex);
    auto iter = queries.find(qid);
    if (iter != queries.end())
        iter->second->trigger_id = timer_service->add(delay_seconds * 1000, [=, this]() noexcept {
            setThreadName("CkptTrigger");
            triggerCheckpoint(qid);
        });
    else
        LOG_WARNING(logger, "Failed to add trigger checkpoint task for query_id={}. The query has been deregistered.", qid);
}

CheckpointCoordinator::TriggerResult
CheckpointCoordinator::triggerCheckpointForQuery(const String & qid, std::function<void(CheckpointContextPtr)> && callback)
{
    CheckpointContextPtr ckpt_ctx;
    {
        std::scoped_lock lock(mutex);

        auto iter = queries.find(qid);
        if (iter == queries.end())
            /// Already canceled the query
            return TriggerResult::Failed;

        ckpt_ctx = iter->second->prepareNextCheckpointContext(std::move(callback));
    }

    return doTriggerCheckpoint(std::move(ckpt_ctx), /*flush=*/true);
}

void CheckpointCoordinator::triggerLastCheckpointAndFlush()
{
    LOG_INFO(logger, "Trigger last checkpoint and flush begin");
    Stopwatch stopwatch;

    auto callback = [this](CheckpointContextPtr ckpt_ctx) {
        LOG_INFO(logger, "Flushed last checkpoint for query={} epoch={}", ckpt_ctx->qid, ckpt_ctx->epoch);
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
    };

    std::vector<CheckpointContextPtr> ckpt_ctxes;

    {
        std::scoped_lock lock(mutex);
        ckpt_ctxes.reserve(queries.size());
        for (auto & [qid, query] : queries)
        {
            if (query->metrics
                && (query->metrics->total_bytes.load() > config.heavy_state_size_threshold
                    || query->metrics->stopwatch.elapsedSeconds() > config.teardown_flush_timeout_sec / 2))
            {
                LOG_INFO(logger, "Skip last checkpoint and flush for query={} since it is a heavy checkpoint", qid);
                continue;
            }

            auto ckpt_ctx = query->prepareNextCheckpointContext(callback);
            ckpt_ctxes.emplace_back(std::move(ckpt_ctx));
        }
    }

    /// <query_id, triggered_epoch>
    std::vector<std::pair<std::string, CheckpointEpoch>> triggered_queries;
    triggered_queries.reserve(ckpt_ctxes.size());

    while (true)
    {
        for (auto iter = ckpt_ctxes.begin(); iter != ckpt_ctxes.end();)
        {
            auto & ckpt_ctx = *iter;
            if (doTriggerCheckpoint(ckpt_ctx, /*flush=*/true) == TriggerResult::Success)
            {
                triggered_queries.emplace_back(ckpt_ctx->qid, ckpt_ctx->epoch);
                iter = ckpt_ctxes.erase(iter);
            }
            else if (!ckpt_ctx->request_ctx->executor)
            {
                CheckpointContextPtr new_ckpt_ctx;
                {
                    std::lock_guard lock(mutex);
                    auto qid_iter = queries.find(ckpt_ctx->qid);
                    if (qid_iter != queries.end())
                        new_ckpt_ctx = qid_iter->second->prepareNextCheckpointContext(callback);
                }

                if (new_ckpt_ctx)
                {
                    LOG_INFO(logger, "Found in-progress checkpoint epoch={} for query={}, retry later", ckpt_ctx->epoch, ckpt_ctx->qid);
                    ckpt_ctx.swap(new_ckpt_ctx);
                    ++iter;
                }
                else
                {
                    LOG_INFO(logger, "Skip last checkpoint and flush for query={} since it was already cancelled", ckpt_ctx->qid);
                    iter = ckpt_ctxes.erase(iter);
                }
            }
            else
            {
                LOG_ERROR(logger, "Failed to trigger last checkpoint epoch={} for query={}, retry later", ckpt_ctx->epoch, ckpt_ctx->qid);
                ++iter;
            }
        }

        if (ckpt_ctxes.empty() || stopwatch.elapsedSeconds() > config.teardown_flush_timeout_sec / 2)
        {
            for (auto & ckpt_ctx : ckpt_ctxes)
                LOG_INFO(logger, "Failed to trigger last checkpoint and flush for query={} epoch={}", ckpt_ctx->qid, ckpt_ctx->epoch);

            break;
        }

        /// If there are failed ckpt triggers and there are still time, retry them.
        /// To avoid busy loop to fire the ckpt trigger, wait for some time here
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // Wait for last checkpoint flush completed
    while (true)
    {
        for (auto qid_iter = triggered_queries.begin(); qid_iter != triggered_queries.end();)
        {
            std::scoped_lock lock(mutex);
            auto iter = queries.find(qid_iter->first);
            /// Remove when triggerd epoch is commited
            /// NOTE: `commited epoch > triggerd epoch` is possible, if there is new checkpoint triggered after triggered last checkpoint flush
            if (iter == queries.end() || iter->second->last_epoch >= qid_iter->second)
                qid_iter = triggered_queries.erase(qid_iter);
            else
                ++qid_iter;
        }

        if (triggered_queries.empty() || stopwatch.elapsedSeconds() > config.teardown_flush_timeout_sec)
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    stopwatch.stop();
    LOG_INFO(logger, "Trigger last checkpoint and flush end (elapsed {} milliseconds)", stopwatch.elapsedMilliseconds());
}

CheckpointCoordinator::TriggerResult CheckpointCoordinator::doTriggerCheckpoint(CheckpointContextPtr ckpt_ctx, bool flush)
{
    /// Create directory before hand. Then all other processors don't need
    /// check and create target epoch ckpt directory.
    try
    {
        chassert(ckpt_ctx->request_ctx);
        CheckpointableExecutorHolder exec(std::move(ckpt_ctx->request_ctx->executor)); /// Take ownership
        if (!exec)
        {
            {
                std::scoped_lock lock(mutex);
                auto iter = queries.find(ckpt_ctx->qid);
                if (iter != queries.end())
                {
                    auto ack_nodes_descs = iter->second->ack_nodes | std::views::transform([](const auto & node) { return node.second; });
                    LOG_WARNING(
                        logger,
                        "Failed to trigger checkpointing state for query={} epoch={}, since prev checkpoint is still in-progress, "
                        "nodes_to_ack={{{}}}",
                        ckpt_ctx->qid,
                        ckpt_ctx->epoch,
                        fmt::format("{}", fmt::join(ack_nodes_descs, ", ")));
                }
                else
                {
                    LOG_WARNING(
                        logger,
                        "Failed to trigger checkpointing state for query={} epoch={}, since it was already cancelled",
                        ckpt_ctx->qid,
                        ckpt_ctx->epoch);
                }
            }

            /// Not reset current_epoch here, since the query may be still in progress
            /// resetCurrentCheckpointEpoch(ckpt_ctx->qid);
            return TriggerResult::Failed;
        }

        if (flush)
        {
            // No need to modify settings - always sync
        }
        else
        {
            if (!exec->hasProcessedNewDataSinceLastCheckpoint())
            {
                LOG_TRACE(
                    logger,
                    "Skipped checkpointing state for query={} epoch={}, since there is no new data processed",
                    ckpt_ctx->qid,
                    ckpt_ctx->epoch);

                resetCurrentCheckpointEpoch(ckpt_ctx->qid);
                return TriggerResult::Skipped;
            }

            if (isHeavyCheckpoint(ckpt_ctx) && !checkAndSetHeavyCheckpointingInProgress(ckpt_ctx->qid))
            {
                LOG_INFO(
                    logger,
                    "Delay checkpointing state for query={} epoch={}, since there is heavy checkpointing in progress",
                    ckpt_ctx->qid,
                    ckpt_ctx->epoch);

                resetCurrentCheckpointEpoch(ckpt_ctx->qid);
                return TriggerResult::Failed;
            }
        }

        preCheckpoint(ckpt_ctx);

        exec->triggerCheckpoint(ckpt_ctx);

        LOG_INFO(logger, "Triggered checkpointing state for query={} epoch={}", ckpt_ctx->qid, ckpt_ctx->epoch);
        return TriggerResult::Success;
    }
    catch (...)
    {
        /// If the checkpoint was already committed asynchronously but updating the in-memory \last_epoch failed (i.e. commitCurrentCheckpointEpoch()),
        /// try reload the last committed epoch from storage and update in-memory \last_epoch.
        if (getCurrentExceptionCode() == ErrorCodes::CHECKPOINT_ALREADY_EXISTS)
        {
            auto last_committed_epoch = ckpt_ctx->storage.getLastCommittedEpoch(ckpt_ctx);
            bool updated = false;
            {
                std::scoped_lock lock(mutex);
                auto iter = queries.find(ckpt_ctx->qid);
                if (iter != queries.end())
                {
                    if (last_committed_epoch > iter->second->last_epoch)
                    {
                        /// Update last committed epoch in memory
                        iter->second->last_epoch = last_committed_epoch;
                        iter->second->current_epoch.reset();
                        iter->second->ack_nodes = iter->second->ack_nodes_readonly;
                        updated = true;
                    }
                }
            }

            if (updated)
            {
                LOG_INFO(
                    logger,
                    "Checkpoint already exists for query={} epoch={}, found and updated last_committed_epoch={}",
                    ckpt_ctx->qid,
                    ckpt_ctx->epoch,
                    last_committed_epoch);

                resetHeavyCheckpointingInProgress(ckpt_ctx->qid);
                return TriggerResult::Skipped;
            }
        }

        tryLogCurrentException(
            logger, fmt::format("Failed to trigger checkpointing state for query={} epoch={}", ckpt_ctx->qid, ckpt_ctx->epoch));
        resetCurrentCheckpointEpoch(ckpt_ctx->qid);
        return TriggerResult::Failed;
    }
}

void CheckpointCoordinator::removeOldOrExpiredCheckpoints() noexcept
{
    try
    {
        std::vector<CheckpointContextPtr> ckpt_ctxes_to_remove;
        {
            std::scoped_lock lock(mutex);
            ckpt_ctxes_to_remove.reserve(queries.size());
            for (const auto & [qid, query] : queries)
            {
                if (query->last_epoch.epoch > 1)
                    ckpt_ctxes_to_remove.emplace_back(query->ctx->cloneWithEpoch(query->last_epoch));
            }
        }

        if (!ckpt_ctxes_to_remove.empty())
        {
            LOG_INFO(logger, "Found {} old checkpoints to remove", ckpt_ctxes_to_remove.size());
            for (const auto & ckpt_ctx : ckpt_ctxes_to_remove)
                doRemoveCheckpoint(ckpt_ctx);
        }

        /// For raft-scheduled mv, follower nodes will not register mv queries, but still materialize replicated checkpoints locally,
        /// so we need to clean up the old checkpoints
        local_ckpt_storage->removeOldCheckpoints();

        /// Try to remove expired checkpoints since last access
        if (auto now = MonotonicSeconds::now(); now - last_access_ttl_check_ts > static_cast<Int64>(config.last_access_ttl_sec))
        {
            last_access_ttl_check_ts = now;
            LOG_INFO(logger, "Begging to remove expired checkpoints");

            std::vector<const CheckpointStorage *> storages;
            {
                std::scoped_lock lock(ckpt_storages_mutex);
                storages.reserve(ckpt_storages.size());
                for (const auto & [_, storage] : ckpt_storages)
                    storages.push_back(storage.get());
            }

            for (const auto * storage : storages)
            {
                storage->removeExpired(config.last_access_ttl_sec, /*delete_marked=*/true, [this](const String & qid) {
                    std::scoped_lock lock(mutex);
                    return !queries.contains(qid);
                });
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to remove old or expired checkpoints");
    }
}

bool CheckpointCoordinator::updateCheckpointInterval(const String & qid, Int64 & interval)
{
    CheckpointContextPtr ckpt_ctx_to_remove;
    {
        std::scoped_lock lock(mutex);
        auto iter = queries.find(qid);
        if (iter == queries.end())
        {
            LOG_INFO(logger, "Skipped to update checkpoint interval to {} in seconds, since the query is not registered yet", interval);
            return false;
        }

        if (interval < 0)
        {
            /// Disable checkpoint explicitly, not cancel the pipeline
            if (iter->second->trigger_id)
            {
                iter->second->trigger_id->cancel();
                iter->second->trigger_id = nullptr;
            }

            ckpt_ctx_to_remove = iter->second->ctx;
            queries.erase(iter);
            LOG_INFO(logger, "Disabled checkpointing for query={}", qid);
        }
        else if (static_cast<UInt64>(interval) != iter->second->settings->interval)
        {
            /// Update checkpoint interval
            iter->second->settings->interval = interval;

            if (iter->second->trigger_id)
                iter->second->trigger_id->cancel();

            iter->second->trigger_id = timer_service->add(iter->second->checkpointInterval(config) * 1000, [this, qid]() noexcept {
                setThreadName("CkptTrigger");
                triggerCheckpoint(qid);
            });
            LOG_INFO(logger, "Updated checkpoint interval to {} in seconds for query={}", interval, qid);
        }
    }

    if (ckpt_ctx_to_remove)
    {
        pool->scheduleOrThrowOnError([this, ckpt_ctx = std::move(ckpt_ctx_to_remove)] {
            setThreadName("CkptCleaner");
            doRemoveCheckpoint(std::move(ckpt_ctx));
        });
    }

    return true;
}

bool CheckpointCoordinator::isHeavyCheckpoint(const CheckpointContextPtr & ckpt_ctx) const
{
    if (auto logstore_ckpt_ctx = ckpt_ctx->tryGetExtra<LogStoreCheckpointContext>())
        return logstore_ckpt_ctx->heavy_state_hint;

    return false;
}

bool CheckpointCoordinator::checkAndSetHeavyCheckpointingInProgress(const String & qid)
{
    std::scoped_lock lock(heavy_checkpointing_mutex);
    if (!heavy_checkpointing_in_progress.has_value())
    {
        heavy_checkpointing_start_ts = MonotonicSeconds::now();
        heavy_checkpointing_in_progress = qid;
        return true;
    }

    /// If a heavy checkpoint lasts for more than 10 minutes, it's probably a bug, so we reset it
    auto now = MonotonicSeconds::now();
    auto elapsed = now - heavy_checkpointing_start_ts;
    if (elapsed > 600)
    {
        LOG_WARNING(
            logger,
            "Heavy checkpointing has been in progress for {} seconds, qid={}, This is likely due to an internal bug. "
            "If this message persists, please contact support for further assistance.",
            elapsed,
            *heavy_checkpointing_in_progress);

        heavy_checkpointing_start_ts = now;
        heavy_checkpointing_in_progress = qid;
        return true;
    }

    return false;
}

void CheckpointCoordinator::resetHeavyCheckpointingInProgress(const String & qid)
{
    std::scoped_lock lock(heavy_checkpointing_mutex);
    if (heavy_checkpointing_in_progress.has_value() && *heavy_checkpointing_in_progress == qid)
    {
        heavy_checkpointing_in_progress.reset();
        heavy_checkpointing_start_ts = 0;
    }
}

void CheckpointCoordinator::resetCurrentCheckpointEpoch(const String & qid)
{
    resetHeavyCheckpointingInProgress(qid);

    {
        std::scoped_lock lock(mutex);
        auto iter = queries.find(qid);
        if (iter == queries.end())
            /// Already canceled the query
            return;

        iter->second->current_epoch.reset();
        iter->second->ack_nodes = iter->second->ack_nodes_readonly;
    }
}

}
