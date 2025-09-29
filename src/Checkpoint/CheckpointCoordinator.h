#pragma once

#include <Checkpoint/Checkpoint.h>
#include <Checkpoint/CheckpointConfig.h>
#include <Checkpoint/CheckpointSettings.h>
#include <Checkpoint/CheckpointStorage.h>
#include <Checkpoint/CheckpointableQuery.h>

#include <Cluster/Common/TimeWheel/TimerService.h>

#include <absl/container/flat_hash_map.h>

#include <filesystem>

namespace Poco
{
class Logger;
}

namespace DB
{
class LocalFileSystemCheckpointStorage;
class PipelineExecutor;

/// Coordinating state checkpointing for all streaming queries
/// on this local node.

class CheckpointCoordinator final
{
public:
    static CheckpointCoordinator & instance(CheckpointConfig config_)
    {
        static CheckpointCoordinator ckpt_coordinator(std::move(config_));
        return ckpt_coordinator;
    }

    explicit CheckpointCoordinator(CheckpointConfig config_);
    ~CheckpointCoordinator();

    const CheckpointConfig & getConfig() const { return config; }

    void startup();
    void shutdown();

    void registerQuery(
        CheckpointContextPtr ckpt_ctx,
        const String & query,
        CheckpointSettingsPtr ckpt_settings,
        std::weak_ptr<PipelineExecutor> executor,
        std::optional<Int64> recovered_ckpt_epoch);

    void deregisterQuery(const String & qid);

    bool isRegistered(const String & qid) const;

    /// Remove checkpoints from checkpoint storage
    void removeCheckpoint(CheckpointContextPtr ckpt_ctx, bool sync = false);

    /// Checkpoint states. We assume ckpt threads will not step on other's ckpt files
    void checkpoint(UInt32 node_id, CheckpointPtr ckpt, CheckpointContextPtr ckpt_ctx);
    void checkpointed(VersionType version, UInt32 node_id, CheckpointContextPtr ckpt_ctx);

    /// Recover states from checkpoint. We assume recover thread will not overlap ckpt threads
    CheckpointPtr recover(UInt32 node_id, CheckpointContextPtr ckpt_ctx) const;

    /// [Legacy] checkpoint/recover state from file checkpoint
    void checkpoint(VersionType version, UInt32 node_id, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt);
    void recover(UInt32 node_id, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType version, ReadBuffer &)> do_recover) const;

    /// Check if the \qid query has been subscribed (i.e. ckpt directory exists)
    bool exist(CheckpointContextPtr ckpt_ctx) const;

    /// Get SELECT QUERY statement for a query ID
    String getQuery(CheckpointContextPtr ckpt_ctx) const;

    /// Get the checkpoint settings for a query
    CheckpointSettingsPtr tryGetCheckpointSettings(CheckpointContextPtr ckpt_ctx) const;

    CheckpointableExecutorHolder getExecutorHolder(const String & qid) const;

    /// Recover the graph for a query, only used in PipelineExecutor::recover()
    void recoverGraph(CheckpointContextPtr ckpt_ctx, std::function<void(VersionType, ReadBuffer &)> do_recover) const;

    void triggerLastCheckpointAndFlush();

    enum class TriggerResult : uint8_t
    {
        Success = 0,
        Skipped = 1,
        Failed = 2
    };

    /// \brief Trigger checkpoint for a query, and call the callback when the checkpoint is done.
    TriggerResult triggerCheckpointForQuery(const String & qid, std::function<void(CheckpointContextPtr)> && callback);

    const CheckpointStorage & getCheckpointStorage(CheckpointReplicationType ckpt_replication_type) const;


    UInt64 getStorageSize(CheckpointContextPtr ckpt_ctx) const;
    PathSizes getStorageStat(CheckpointContextPtr ckpt_ctx) const;
    CheckpointRequestMetricsPtr getCheckpointRequestMetrics(CheckpointContextPtr ckpt_ctx) const;

    /// \return true if the checkpoint interval is updated
    bool updateCheckpointInterval(const String & qid, Int64 & interval);

private:
    void triggerCheckpointAfter(uint64_t delay_seconds, const String & qid);

    void triggerCheckpoint(const String & qid) noexcept;
    void removeOldOrExpiredCheckpoints() noexcept;
    void doRemoveCheckpoint(CheckpointContextPtr ckpt_ctx) noexcept;

    bool commitCurrentCheckpointEpochWithLock(CheckpointContextPtr ckpt_ctx) TSA_REQUIRES(mutex);

    TriggerResult doTriggerCheckpoint(CheckpointContextPtr ckpt_ctx, bool flush = false);

    void resetCurrentCheckpointEpoch(const String & qid);

    bool isHeavyCheckpoint(const CheckpointContextPtr & ckpt_ctx) const;
    bool checkAndSetHeavyCheckpointingInProgress(const String & qid);
    void resetHeavyCheckpointingInProgress(const String & qid);

    void preCheckpoint(CheckpointContextPtr ckpt_ctx);
    void checkpoint(const String & key, CheckpointPtr ckpt, CheckpointContextPtr ckpt_ctx);
    void checkpoint(VersionType version, const String & key, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt);
    void recover(
        const String & key,
        CheckpointContextPtr ckpt_ctx,
        std::function<void(VersionType version, ReadBuffer &)> do_recover,
        bool log = true) const;

    /// \return true if the checkpoint is committed, otherwise, it is just pre-committed
    bool commit(CheckpointContextPtr ckpt_ctx) TSA_REQUIRES(mutex);


    void validateCheckpointSettings(const CheckpointSettingsPtr & ckpt_settings, const CheckpointContextPtr & ckpt_ctx);

private:
    static constexpr String QUERY_CKPT_FILE = "query";
    static constexpr String SETTINGS_CKPT_FILE = "settings";
    static constexpr String GRAPH_CKPT_FILE = "dag";

    CheckpointConfig config;

    mutable std::mutex ckpt_storages_mutex;
    mutable absl::flat_hash_map<CheckpointReplicationType, std::unique_ptr<const CheckpointStorage>>
        ckpt_storages TSA_GUARDED_BY(ckpt_storages_mutex);
    const LocalFileSystemCheckpointStorage * local_ckpt_storage; /// Used for async checkpointing

    /// Query UUID -> QueryContext mapping
    mutable std::mutex mutex;
    absl::flat_hash_map<String, CheckpointableQueryPtr> queries TSA_GUARDED_BY(mutex);

    std::unique_ptr<cluster::TimerService> timer_service;
    std::unique_ptr<ThreadPool> pool;

    std::atomic_flag started;
    std::atomic_flag stopped;

    std::mutex heavy_checkpointing_mutex;
    /// The values is qid of heavy checkpointing query
    std::optional<String> heavy_checkpointing_in_progress;
    Int64 heavy_checkpointing_start_ts = 0; /// (seconds)

    Int64 last_access_ttl_check_ts = 0; /// (seconds)

    LoggerPtr logger;
};
}
