#pragma once

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointSettings.h>

#include <Cluster/Common/TimeWheel/TimerService.h>
#include <Core/PathSize.h>
#include <Processors/Executors/PipelineExecutor.h>

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <mutex>

namespace DB
{
struct CheckpointLease;
struct CheckpointConfig;
class CheckpointStorage;

struct CheckpointableQuery
{
    CheckpointableQuery(
        const CheckpointStorage & storage_,
        CheckpointSettingsPtr settings_,
        std::weak_ptr<PipelineExecutor> executor_,
        std::optional<CheckpointEpoch> recovered_epoch,
        CheckpointContextPtr ctx_);

    /// Return true if all ack nodes acked
    bool ack(UInt32 node_id);

    /// Only called when the checkpoint committed successfully
    void prepareForNextEpoch();

    CheckpointContextPtr prepareNextCheckpointContext(std::function<void(CheckpointContextPtr)> && callback = {});

    CheckpointLease checkpointLease() const;

    UInt64 checkpointInterval(const CheckpointConfig & config) const;

    String checkpointAckNodeDescriptions() const;

    const CheckpointStorage & storage;

    CheckpointSettingsPtr settings;

    CheckpointableExecutorHolder executor;

    CheckpointContextPtr ctx;

    CheckpointRequestMetricsPtr metrics; /// Last committed epoch metrics

    absl::flat_hash_map<UInt32, String /*node_desc*/> ack_nodes_readonly;
    absl::flat_hash_map<UInt32, String /*node_desc*/> ack_nodes;

    CheckpointEpoch current_epoch{.epoch = 0};
    CheckpointEpoch last_epoch{.epoch = -1};

    cluster::TimerTaskEntryPtr trigger_id;

    std::shared_ptr<std::atomic_flag> async_replication_finished;

    bool is_lightweight = false;

    mutable std::mutex metrics_cache_mutex;
    uint64_t cached_storage_size TSA_GUARDED_BY(metrics_cache_mutex) = 0;
    int64_t last_cached_ts TSA_GUARDED_BY(metrics_cache_mutex) = 0;
    std::atomic_flag size_refresh_scheduled;

    PathSizes cached_storage_stat TSA_GUARDED_BY(metrics_cache_mutex);
    int64_t last_stat_cached_ts TSA_GUARDED_BY(metrics_cache_mutex) = 0;
    std::atomic_flag stat_refresh_scheduled;
};
using CheckpointableQueryPtr = std::shared_ptr<CheckpointableQuery>;
}
