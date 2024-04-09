#pragma once

#include "CheckpointStorage.h"

#include <Interpreters/Context_fwd.h>
#include <Common/TimerService.h>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <filesystem>

namespace Poco
{
class Logger;
}

namespace DB
{
class PipelineExecutor;

struct CheckpointableQuery;

using CheckpointableQueryPtr = std::unique_ptr<CheckpointableQuery>;

/// Coordinating state checkpointing for all streaming queries
/// on this local node.

class CheckpointCoordinator final
{
public:
    static CheckpointCoordinator & instance(ContextPtr global_context)
    {
        static CheckpointCoordinator ckpt_coordinator(global_context);
        return ckpt_coordinator;
    }

    explicit CheckpointCoordinator(ContextPtr global_context);
    ~CheckpointCoordinator();

    void startup();
    void shutdown();

    void registerQuery(
        const String & qid,
        const String & query,
        UInt64 ckpt_interval,
        std::weak_ptr<PipelineExecutor> executor,
        std::optional<Int64> recovered_epoch = {});

    void deregisterQuery(const String & qid);
    /// Remove checkpoints from checkpoint storage
    void removeCheckpoint(const String & qid);

    /// Checkpoint states. We assume ckpt threads will not step on other's ckpt files
    void preCheckpoint(CheckpointContextPtr ckpt_ctx);
    void checkpoint(VersionType version, const String & key, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt);
    void checkpoint(VersionType version, UInt32 node_id, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt);
    void checkpointed(VersionType version, UInt32 node_id, CheckpointContextPtr ckpt_ctx);

    /// Get SELECT QUERY statement for a query ID
    String getQuery(const String & qid);

    /// Recover states from checkpoint. We assume recover thread will not overlap ckpt threads
    void recover(const String & qid, std::function<void(CheckpointContextPtr)> do_recover);
    void recover(const String & key, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType version, ReadBuffer &)> do_recover);
    void recover(UInt32 node_id, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType version, ReadBuffer &)> do_recover);
    void recovered(CheckpointContextPtr ckpt_ctx, UInt64 ckpt_interval, std::shared_ptr<PipelineExecutor> executor);

    void triggerLastCheckpointAndFlush();

private:
    void triggerCheckpoint(const String & qid, UInt64 checkpoint_interval);
    void removeExpiredCheckpoints(bool delete_marked);

    bool doTriggerCheckpoint(const std::weak_ptr<PipelineExecutor> & executor, CheckpointContextPtr ckpt_ctx);

private:
    std::unique_ptr<CheckpointStorage> ckpt;

    UInt64 last_access_ttl;
    UInt64 last_access_check_interval;
    UInt64 grace_interval;
    UInt64 teardown_flush_timeout;

    /// Query UUID -> QueryContext mapping
    mutable std::mutex mutex;
    absl::flat_hash_map<String, CheckpointableQueryPtr> queries TSA_GUARDED_BY(mutex);

    TimerService timer_service;

    std::optional<ThreadPool> pool;

    std::atomic_flag started;
    std::atomic_flag stopped;

    Poco::Logger * logger;
};
}
