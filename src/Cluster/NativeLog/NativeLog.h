#pragma once

#include <Cluster/Common/AckSemantic.h>
#include <Cluster/Common/EpochSequence.h>
#include <Cluster/Common/FetchIsolation.h>
#include <Cluster/Common/ShardReplica.h>
#include <Cluster/Common/ShardSequence.h>
#include <Cluster/Common/ShardTimestamp.h>
#include <Cluster/Common/StreamIDShard.h>
#include <Cluster/Common/StreamShard.h>
#include <Cluster/LocalLog/Common/FetchResult.h>
#include <Cluster/LocalLog/Log/Log.h>
#include <Cluster/LocalLog/Log/LogConfig.h>
#include <Cluster/LocalLog/Log/LogConfigMap.h>
#include <Cluster/LocalLog/Log/LogManager.h>
#include <Cluster/StoreConfig.h>

#include <Core/PathSize.h>
#include <Common/BackgroundSchedulePool.h>

namespace cluster
{
struct AppendResult
{
    AppendResult() = default;
    AppendResult(int64_t sn_, int error_code_) : sn(sn_), error_code(error_code_) { }

    /// Sequence number assigned to the append operation
    int64_t sn = -1;

    /// Error code (0 for success)
    int error_code = 0;

    /// Simple fulfill method for compatibility with existing code
    void fulfill(int64_t assigned_sn, int err_code, const std::string & /* error_msg */)
    {
        sn = assigned_sn;
        error_code = err_code;
    }
};

using AppendResultPtr = std::shared_ptr<AppendResult>;

namespace meta
{
class MetaStore;
}

namespace nlog
{
class LogManager;
}
class NativeLog final
{
public:
    NativeLog(
        StoreConfigPtr config_, meta::MetaStore & meta_store_, DB::NLOG::BackgroundSchedulePool & scheduler, ThreadPool & adhoc_scheduler);

    ~NativeLog() noexcept;

    void startup();
    void shutdown();

    bool isReady() const noexcept { return ready.load(std::memory_order_relaxed); }

    /// Create log on filesystem or update log settings in-memory
    int create(const StreamShard & stream_shard, nlog::LogConfigEntry log_config);

    /// Update log settings
    int alter(
        const std::vector<StreamShard> & stream_shards,
        const std::unordered_map<std::string, int32_t> & flush_settings,
        const std::unordered_map<std::string, int64_t> & retention_settings);

    /// Get stream shard sizes on disk and filesystem paths etc
    CallResult<std::optional<DB::PathSize>> stat(const StreamIDShard & stream_shard) const;
    CallResult<DB::PathSizes> stats(const std::vector<StreamShard> & stream_shards) const;

    /// Get stream shard log max entry size. Return 0 when stream shard not found.
    uint64_t maxEntrySize(const StreamIDShard & stream_shards) const;

    /// \return error code
    int remove(const std::vector<StreamShard> & stream_shards);

    /// Data APIs (Timestamp which are not something essential to Raft algorithm but essential to streaming)
    /// \return error code

    CallResult<AppendResultPtr>
    appendAsync(const StreamIDShard & stream_shard, ByteVector && data, int64_t max_event_time, uint64_t correlation_id);

    CallResult<AppendResultPtr>
    append(const StreamIDShard & stream_shard, ByteVector && data, int64_t max_event_time, AckSemantic ack, int64_t timeout_ms);

    CallResult<nlog::FetchResult> fetch(
        const StreamIDShard & stream_shard,
        int64_t sn,
        uint64_t max_size,
        int64_t max_wait_ms,
        std::optional<FetchHint> fetch_hint,
        FetchIsolation isolation) const;

    ShardSequences translateTimestamps(const Stream & stream, const ShardTimestamps & shard_timestamps, bool append_time);
    nlog::LogPtr getOrCreateLog(const StreamShard & stream_shard);
    nlog::LogPtr getLog(const StreamIDShard & stream_shard) const;
    void shutdown(const StreamIDShard & stream_shard, bool hot_close = false);
    void restart(const StreamIDShard & stream_shard);


    nlog::LogConfigPtr getLogConfig(const StreamIDShard & stream_shard) const;

    const StoreConfig & getConfig() const { return *config; }

    size_t numStreamShards() const noexcept;

    std::optional<int64_t> logStartSequence(const StreamIDShard & stream_shard) const;
    std::optional<int64_t> committedSequence(const StreamIDShard & stream_shard) const;
    void setAppliedSequenceGetter(const StreamIDShard & stream_shard, std::function<int64_t()> applied_sn_getter);

    /// Pause/Resume/Recover specific stream shards
    Error pauseStreamShard(const StreamIDShard & stream_shard);
    Error resumeStreamShard(const StreamIDShard & stream_shard);
    Error recoverStreamShard(const StreamIDShard & stream_shard);

private:
    void init();
    void initLogManager();

private:
    StoreConfigPtr config;

    [[maybe_unused]] meta::MetaStore & meta_store;
    [[maybe_unused]] DB::NLOG::BackgroundSchedulePool & scheduler;
    [[maybe_unused]] ThreadPool & adhoc_scheduler;

    std::atomic_flag started;
    std::atomic_flag stopped;
    std::atomic<bool> ready = false;

    LoggerPtr logger;
    std::unique_ptr<nlog::LogManager> log_manager;
};
}
