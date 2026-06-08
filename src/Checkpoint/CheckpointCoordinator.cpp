#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/CheckpointStorageFactory.h>
#include <Checkpoint/LocalFileSystemCheckpointStorage.h>
#include <Checkpoint/LogStoreCheckpointContext.h>

#include <Bootstrap/Globals.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/getFileSystemSpace.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include <Common/setThreadName.h>

#include <base/sleep.h>

namespace CurrentMetrics
{
extern const Metric CheckpointCoordinatorThread;
extern const Metric CheckpointCoordinatorThreadActive;
extern const Metric AsyncCheckpointThreads;
extern const Metric AsyncCheckpointThreadsActive;
}

namespace DB
{
namespace ErrorCodes
{
extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
extern const int INVALID_DISK;
}

CheckpointCoordinator::CheckpointCoordinator(CheckpointConfig config_)
    : config(std::move(config_)), logger(getLogger("CheckpointCoordinator"))
{
    local_ckpt_storage
        = assert_cast<const LocalFileSystemCheckpointStorage *>(&getCheckpointStorage(CheckpointReplicationType::LocalFileSystem));

    const auto & path_str = config.path;

    auto [total_space, free_space, available_space, used_space] = getFileSystemSpace(path_str);

    LOG_INFO(
        logger,
        "The path of checkpoint is {}. The device where this path is located has total space: {}, free space: {}, available space: {}, "
        "used space: {}.",
        path_str,
        formatReadableSizeWithBinarySuffix(total_space),
        formatReadableSizeWithBinarySuffix(free_space),
        formatReadableSizeWithBinarySuffix(available_space),
        formatReadableSizeWithBinarySuffix(used_space));
}

CheckpointCoordinator::~CheckpointCoordinator()
{
    shutdown();
}

void CheckpointCoordinator::startup()
{
    if (started.test_and_set())
        return;

    /// first level range [1 sec, 1 sec * 8000 slots = 2.22 hours]
    /// to save memory for last_access_check_interval_sec = 2 hours
    timer_service = std::make_unique<cluster::TimerService>(
        /*thread_pool_size=*/4, /*tick_ms=*/1000, /*wheel_size=*/8000, /*expired_timer_size=*/1000);

    pool = std::make_unique<ThreadPool>(
        CurrentMetrics::CheckpointCoordinatorThread,
        CurrentMetrics::CheckpointCoordinatorThreadActive,
        /*max_threads=*/4,
        /*max_free_threads=*/4,
        /*queue_size=*/1000,
        /*shutdown_on_exception=*/false);

    /// Every delete_grace_interval_sec (default 60 seconds), remove old checkpoints
    [[maybe_unused]] auto timer1 = timer_service->add(
        config.delete_grace_interval_sec * 1000,
        [this]() {
            pool->scheduleOrThrow([this] {
                try
                {
                    setThreadName("CkptExpirer");
                    removeOldOrExpiredCheckpoints();
                }
                catch (...)
                {
                    tryLogCurrentException(logger, "Caught exception during removal of expired checkpoints");
                }
            });
        },
        /*repeat=*/true);
    chassert(timer1);
}

void CheckpointCoordinator::shutdown()
{
    if (stopped.test_and_set())
        return;

    if (timer_service)
    {
        timer_service->shutdown();
        /// Release the timer service resources (i.e. TimerTaskEntries) before the dtor() of global CheckpointCoordinator
        timer_service.reset();
    }

    if (pool)
    {
        pool->wait();
        pool.reset();
    }
}

void CheckpointCoordinator::validateCheckpointSettings(
    const CheckpointSettingsPtr & ckpt_settings, [[maybe_unused]] const CheckpointContextPtr & ckpt_ctx)
{
    if (ckpt_settings->replication_type != CheckpointReplicationType::LocalFileSystem)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only LocalFileSystem checkpoint storage is supported");
    }
}

const CheckpointStorage & CheckpointCoordinator::getCheckpointStorage(CheckpointReplicationType ckpt_replication_type) const
{
    std::scoped_lock lock(ckpt_storages_mutex);
    auto iter = ckpt_storages.find(ckpt_replication_type);
    if (iter != ckpt_storages.end())
        return *iter->second;

    auto ckpt_storage = CheckpointStorageFactory::create(ckpt_replication_type, config);
    auto [it, inserted] = ckpt_storages.emplace(ckpt_replication_type, std::move(ckpt_storage));
    chassert(inserted);
    return *it->second;
}

void CheckpointCoordinator::registerQuery(
    CheckpointContextPtr ckpt_ctx,
    const String & query,
    CheckpointSettingsPtr ckpt_settings,
    std::weak_ptr<PipelineExecutor> executor,
    std::optional<CheckpointEpoch> recovered_ckpt_epoch)
{
    chassert(ckpt_ctx->epoch.empty());
    const auto & qid = ckpt_ctx->qid;
    const auto & ckpt_storage = getCheckpointStorage(ckpt_settings->replication_type);
    if (!ckpt_storage.isLocal() && !ckpt_ctx->extra_ctx)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Missing extra checkpoint context for non-local checkpoint replication type '{}'",
            ckpt_settings->replication_type);

    ckpt_settings->replication_type = ckpt_storage.replicationType(); /// Update replication type (E.g. Auto -> NativeLog)

    validateCheckpointSettings(ckpt_settings, ckpt_ctx);

    /// Use the configured interval if the user does not specify
    if (ckpt_settings->interval == 0)
        ckpt_settings->interval = config.interval_sec;

    auto ckpt_query = std::make_unique<CheckpointableQuery>(ckpt_storage, ckpt_settings, executor, recovered_ckpt_epoch, ckpt_ctx);
    auto ack_node_descs = ckpt_query->checkpointAckNodeDescriptions();
    auto ckpt_interval = ckpt_query->checkpointInterval(config);

    bool query_exists = false;
    {
        std::scoped_lock lock(mutex);

        auto [_, inserted] = queries.emplace(qid, std::move(ckpt_query));
        query_exists = !inserted;
    }

    if (query_exists)
    {
        LOG_INFO(logger, "Failed to register query={} since the query id already exists", qid);
        throw Exception(
            ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING, "Failed to register query={} since the query id already exists", qid);
    }

    if (!recovered_ckpt_epoch.has_value())
    {
        preCheckpoint(ckpt_ctx);

        /// First persist the graph in a file on \ckpt_storage.
        {
            auto exec = executor.lock();
            chassert(exec);
            checkpoint(exec->getVersion(), GRAPH_CKPT_FILE, ckpt_ctx, [&](WriteBuffer & wb) { exec->checkpointGraph(wb); });
        }

        /// Then persist the query in a different file on \ckpt_storage.
        /// We choose to persist query here for easier to recover the query from the ckpt
        checkpoint(1, QUERY_CKPT_FILE, ckpt_ctx, [&](WriteBuffer & wb) {
            /// Query
            DB::writeStringBinary(query, wb);
        });

        /// Persist the checkpoint settings in a different file on \ckpt_storage.
        /// We choose to persist settings here for easier to recover the settings from the ckpt
        checkpoint(CheckpointSettings::VERSION, SETTINGS_CKPT_FILE, ckpt_ctx, [&](WriteBuffer & wb) {
            ckpt_settings->serialize(CheckpointSettings::VERSION, wb);
        });
    }

    LOG_INFO(
        logger,
        "Register query={} with default_ckpt_interval={}, revised_ckpt_interval={}, ack_node_descriptions={{{}}}",
        qid,
        ckpt_settings->interval,
        ckpt_interval,
        ack_node_descs);

    triggerCheckpointAfter(ckpt_interval, qid);
}

void CheckpointCoordinator::deregisterQuery(const String & qid)
{
    bool deregistered = false;
    CheckpointableExecutorHolder executor;
    CheckpointContextPtr ckpt_ctx;
    std::shared_ptr<std::atomic_flag> async_replication_finished;
    {
        std::scoped_lock lock(mutex);

        auto iter = queries.find(qid);
        if (iter != queries.end())
        {
            if (iter->second->trigger_id)
                iter->second->trigger_id->cancel();

            async_replication_finished.swap(iter->second->async_replication_finished);
            executor.swap(iter->second->executor);
            ckpt_ctx = iter->second->ctx;

            queries.erase(iter);
            deregistered = true;
        }
    }

    if (deregistered)
    {
        /// Wait for in-progress async checkpoint replication to complete before deregistering.
        /// This prevents epoch conflicts during materialized view recovery.
        if (async_replication_finished && !async_replication_finished->test())
        {
            LOG_INFO(logger, "Waiting for async replication finished for query={} ...", qid);
            async_replication_finished->wait(false);
        }

        /// The query has been deregistered. Wait for executor's reference goes to 1.
        /// Usually ongoing triggering tasks (i.e. `doTriggerCheckpoint()`) will ref executor (so wait for them to complete)
        /// Doing this here because we must make sure pipeline executor is released before QueryStatus's dtor,
        /// check issue https://github.com/timeplus-io/proton-enterprise/issues/5829
        size_t retries = 0;
        while (executor && !executor.unique())
        {
            /// Broken it if it takes too long greater than 30s, which is unlikely to happen
            if (retries >= 3'000)
            {
                LOG_ERROR(logger, "Timeout while waiting executor's reference decreased to 1 for query={}", qid);
                break;
            }

            ++retries;

            /// Log a warning every 1s
            if (retries % 100 == 0)
                LOG_WARNING(logger, "Waiting for executor's reference decreased to 1 for query={} ...", qid);

            sleepForMilliseconds(5);
        }

        resetHeavyCheckpointingInProgress(qid);

        LOG_INFO(logger, "Deregistered query={}", qid);
    }
}

void CheckpointCoordinator::removeCheckpoint(CheckpointContextPtr ckpt_ctx, bool sync)
{
    LOG_INFO(logger, "Going to clean checkpoints for query={}", ckpt_ctx->qid);

    CheckpointableExecutorHolder executor;
    {
        /// If query with qid is running, cancel it first
        std::scoped_lock lock(mutex);
        auto iter = queries.find(ckpt_ctx->qid);
        if (iter != queries.end())
            executor = iter->second->executor;
    }

    if (executor)
        executor->cancel();

    if (sync)
    {
        doRemoveCheckpoint(ckpt_ctx);
    }
    else
    {
        pool->scheduleOrThrowOnError([this, ckpt_ctx] {
            setThreadName("CkptCleaner");
            doRemoveCheckpoint(ckpt_ctx);
        });
    }
}

void CheckpointCoordinator::doRemoveCheckpoint(CheckpointContextPtr ckpt_ctx) noexcept
{
    try
    {
        ckpt_ctx->storage.remove(ckpt_ctx);

        if (!ckpt_ctx->storage.isLocal())
            local_ckpt_storage->remove(ckpt_ctx);

        if (ckpt_ctx->epoch.empty())
            LOG_INFO(logger, "Cleaned checkpoints for query={}", ckpt_ctx->qid);
    }
    catch (...)
    {
        tryLogCurrentException(logger, fmt::format("Failed to remove checkpoints for query={}", ckpt_ctx->qid));
    }
}

bool CheckpointCoordinator::exist(CheckpointContextPtr ckpt_ctx) const
{
    return local_ckpt_storage->exists(QUERY_CKPT_FILE, ckpt_ctx) || ckpt_ctx->storage.exists(QUERY_CKPT_FILE, ckpt_ctx);
}

String CheckpointCoordinator::getQuery(CheckpointContextPtr ckpt_ctx) const
{
    if (!ckpt_ctx->storage.exists(QUERY_CKPT_FILE, ckpt_ctx))
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Can't find query={}", ckpt_ctx->qid);

    String query;
    recover(QUERY_CKPT_FILE, std::move(ckpt_ctx), [&query](VersionType /*version*/, ReadBuffer & rb) { DB::readStringBinary(query, rb); });

    return query;
}

CheckpointSettingsPtr CheckpointCoordinator::tryGetCheckpointSettings(CheckpointContextPtr ckpt_ctx) const
{
    if (!ckpt_ctx)
        return nullptr;

    /// Get from memory first
    {
        std::scoped_lock lock(mutex);
        auto iter = queries.find(ckpt_ctx->qid);
        if (iter != queries.end())
            return iter->second->settings;
    }

    /// Get from ckpt meta storage
    /// Legacy checkpoints may not have the settings file
    if (!ckpt_ctx->storage.exists(SETTINGS_CKPT_FILE, ckpt_ctx))
        return nullptr;

    /// Version + Properties ...
    auto ckpt_settings = std::make_shared<CheckpointSettings>();
    recover(
        SETTINGS_CKPT_FILE,
        std::move(ckpt_ctx),
        [&](VersionType version, ReadBuffer & rb) { ckpt_settings->deserialize(version, rb); },
        /*log=*/false);
    return ckpt_settings;
}

bool CheckpointCoordinator::isRegistered(const String & qid) const
{
    std::scoped_lock lock(mutex);
    return queries.contains(qid);
}

CheckpointableExecutorHolder CheckpointCoordinator::getExecutorHolder(const String & qid) const
{
    std::scoped_lock lock(mutex);
    auto iter = queries.find(qid);
    if (iter == queries.end())
        return {};

    return iter->second->executor;
}

UInt64 CheckpointCoordinator::getStorageSize(CheckpointContextPtr ckpt_ctx) const
{
    if (!ckpt_ctx)
        return 0;

    /// Return cached storage size if exists and not expired (30 mins)
    {
        std::scoped_lock lock(mutex);
        auto iter = queries.find(ckpt_ctx->qid);
        if (iter == queries.end())
            return 0;

        if (DB::MonotonicSeconds::now() - iter->second->last_cached_ts <= 30 * 60) /// 30 mins
            return iter->second->cached_storage_size;
    }

    auto storage_size = ckpt_ctx->storage.getStorageSize(ckpt_ctx);

    /// Update cached storage size
    {
        std::scoped_lock lock(mutex);
        auto iter = queries.find(ckpt_ctx->qid);
        if (iter != queries.end())
        {
            iter->second->cached_storage_size = storage_size;
            iter->second->last_cached_ts = DB::MonotonicSeconds::now();
        }
    }
    return storage_size;
}

PathSizes CheckpointCoordinator::getStorageStat(CheckpointContextPtr ckpt_ctx) const
{
    if (!ckpt_ctx)
        return {};

    return ckpt_ctx->storage.getStorageStat(ckpt_ctx);
}

CheckpointRequestMetricsPtr CheckpointCoordinator::getCheckpointRequestMetrics(CheckpointContextPtr ckpt_ctx) const
{
    if (!ckpt_ctx)
        return nullptr;

    std::scoped_lock lock(mutex);
    auto iter = queries.find(ckpt_ctx->qid);
    if (iter == queries.end())
        return nullptr;

    return iter->second->metrics;
}

}
