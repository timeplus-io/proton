#include <Cluster/LocalLog/Common/FetchResult.h>
#include <Cluster/LocalLog/Common/LogSequenceMetadata.h>
#include <Cluster/LocalLog/Log/Log.h>
#include <Cluster/LocalLog/Log/LogConfig.h>
#include <Cluster/LocalLog/Log/LogConfigMap.h>
#include <Cluster/LocalLog/Log/LogManager.h>
#include <Cluster/LocalLog/Log/LogManagerConfig.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Cluster/Common/Entry.h>

#include <Bootstrap/Globals.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
extern const int CANNOT_LOAD_CONFIG;
extern const int UNKNOWN_STREAM;
extern const int INGEST_TIMEOUT;
extern const int NOT_IMPLEMENTED;
extern const int OK;
}

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}


namespace cluster
{

int NativeLog::create(const StreamShard & stream_shard, nlog::LogConfigEntry log_config_entry)
{
    if (!log_manager)
        return DB::ErrorCodes::NOT_IMPLEMENTED;

    /// Use the LogConfig from LogConfigEntry, or default if not provided
    auto log_config
        = log_config_entry.config ? log_config_entry.config : std::make_shared<nlog::LogConfig>(log_manager->currentDefaultConfig());

    auto log = log_manager->getOrCreateLog(stream_shard, log_config);
    return log ? DB::ErrorCodes::OK : DB::ErrorCodes::UNKNOWN_STREAM;
}

nlog::LogPtr NativeLog::getOrCreateLog(const StreamShard & stream_shard)
{
    if (!log_manager)
    {
        LOG_ERROR(logger, "getOrCreateLog called but log_manager is null for stream_shard {}", stream_shard.string());
        return nullptr;
    }

    /// Use LogManager's default config
    auto log_config = std::make_shared<nlog::LogConfig>(log_manager->currentDefaultConfig());

    auto log = log_manager->getOrCreateLog(stream_shard, log_config);
    if (!log)
    {
        LOG_ERROR(logger, "LogManager::getOrCreateLog returned null for stream_shard {}", stream_shard.string());
    }
    else
    {
        LOG_INFO(logger, "Successfully got/created log for stream_shard {}", stream_shard.string());
    }

    return log;
}

nlog::LogPtr NativeLog::getLog(const StreamIDShard & stream_shard) const
{
    if (!log_manager)
        return nullptr;

    return log_manager->getLog(stream_shard);
}


int NativeLog::alter(
    const std::vector<StreamShard> & stream_shards,
    const std::unordered_map<std::string, int32_t> & flush_settings,
    const std::unordered_map<std::string, int64_t> & retention_settings)
{
    if (!log_manager)
        return DB::ErrorCodes::NOT_IMPLEMENTED;

    /// For each stream shard, update its configuration
    for (const auto & stream_shard : stream_shards)
    {
        auto log = getLog(stream_shard.id_shard);
        if (!log)
            continue;

        /// Update the log configuration directly
        log->updateConfig(flush_settings, retention_settings);
    }

    return DB::ErrorCodes::OK;
}

int NativeLog::remove(const std::vector<StreamShard> & stream_shards)
{
    if (!log_manager)
        return DB::ErrorCodes::NOT_IMPLEMENTED;

    log_manager->remove(stream_shards);
    return DB::ErrorCodes::OK;
}

CallResult<std::optional<DB::PathSize>> NativeLog::stat(const StreamIDShard & stream_shard) const
{
    if (log_manager)
    {
        auto log = log_manager->getLog(stream_shard);
        if (!log)
            return CallResult<std::optional<DB::PathSize>>(DB::ErrorCodes::UNKNOWN_STREAM); /// failure.

        return CallResult<std::optional<DB::PathSize>>(DB::PathSize{log->logDir().string(), log->size()});
    }

    return CallResult<std::optional<DB::PathSize>>(std::nullopt);
}

CallResult<DB::PathSizes> NativeLog::stats(const std::vector<StreamShard> & stream_shards) const
{
    DB::PathSizes stats;
    stats.reserve(stream_shards.size());

    for (const auto & stream_shard : stream_shards)
    {
        auto stat_result = stat(stream_shard.id_shard);
        if (stat_result.hasError())
            continue;

        assert(stat_result.result.has_value());
        stats.push_back(std::move(stat_result.result.value()));
    }

    return CallResult<DB::PathSizes>(std::move(stats));
}

uint64_t NativeLog::maxEntrySize(const StreamIDShard & stream_shard) const
{
    auto log_config = getLogConfig(stream_shard);
    return log_config != nullptr ? log_config->max_entry_size : config->log_config->max_entry_size;
}

CallResult<AppendResultPtr>
NativeLog::appendAsync(const StreamIDShard & stream_shard, ByteVector && data, int64_t /*max_event_time*/, uint64_t /*correlation_id*/)
{
    /// Get or create the log for this stream shard
    auto log = getLog(stream_shard);
    if (!log)
    {
        /// Need to create StreamShard to call getOrCreateLog
        StreamShard ss;
        ss.id_shard = stream_shard;
        log = getOrCreateLog(ss);
    }

    /// Create entry for Log::appendLocal
    auto entry = std::make_shared<cluster::Entry>();
    entry->data = std::move(data);

    /// Append to local log
    auto result_sn = log->appendLocal(entry);
    if (result_sn.hasError())
    {
        LOG_ERROR(
            logger, "appendAsync: Failed to append to log for stream_shard {}, error={}", stream_shard.string(), result_sn.error_code);
        return CallResult<AppendResultPtr>(result_sn.error_code);
    }

    LOG_TRACE(
        logger, "appendAsync: Successfully appended to log for stream_shard {}, assigned sn={}", stream_shard.string(), result_sn.result);

    auto append_result = std::make_shared<AppendResult>();

    // Immediately fulfill the append since operation is complete
    append_result->fulfill(result_sn.result, 0, "");

    return CallResult<AppendResultPtr>(append_result);
}

CallResult<AppendResultPtr>
NativeLog::append(const StreamIDShard & stream_shard, ByteVector && data, int64_t max_event_time, AckSemantic ack, int64_t timeout_ms)
{
    (void)ack;
    (void)timeout_ms;

    return appendAsync(stream_shard, std::move(data), max_event_time, 0);
}

CallResult<nlog::FetchResult> NativeLog::fetch(
    const StreamIDShard & stream_shard,
    int64_t sn,
    uint64_t max_size,
    int64_t max_wait_ms,
    std::optional<FetchHint> fetch_hint,
    FetchIsolation isolation) const
{
    auto log = getLog(stream_shard);
    if (!log)
    {
        /// Log doesn't exist yet
        nlog::FetchResult empty_result;
        return CallResult<nlog::FetchResult>(empty_result);
    }

    /// Fetch from the log
    return log->fetch(sn, max_size, max_wait_ms, fetch_hint, isolation);
}

ShardSequences NativeLog::translateTimestamps(const Stream & stream, const ShardTimestamps & shard_timestamps, bool append_time)
{
    ShardSequences shard_sequences;
    shard_sequences.reserve(shard_timestamps.size());

    if (log_manager)
    {
        for (const auto & shard_timestamp : shard_timestamps)
        {
            /// Let the LogManager pick the right log to do the timestamp lookup
            if (auto log = log_manager->getLog(StreamIDShard{stream.id, shard_timestamp.shard}); log)
            {
                shard_sequences.emplace_back(shard_timestamp.shard, log->sequenceForTimestamp(shard_timestamp.timestamp, append_time));
            }
            else
            {
                LOG_ERROR(logger, "Failed to translate timestamp for stream {} since it doesn't exist", stream.name);
                throw DB::Exception(DB::ErrorCodes::UNKNOWN_STREAM, "Stream {} doesn't exist", stream.name);
            }
        }
    }
    else
    {
        /// No log_manager, return empty sequences
        for (const auto & shard_timestamp : shard_timestamps)
        {
            shard_sequences.emplace_back(shard_timestamp.shard, 0);
        }
    }

    return shard_sequences;
}

std::optional<int64_t> NativeLog::logStartSequence(const StreamIDShard & stream_shard) const
{
    if (log_manager)
    {
        if (auto log = log_manager->getLog(stream_shard); log)
            return log->logStartSequence();
    }

    return std::nullopt;
}

std::optional<int64_t> NativeLog::committedSequence(const StreamIDShard & stream_shard) const
{
    if (log_manager)
    {
        if (auto log = log_manager->getLog(stream_shard); log)
            return log->committedSequence();
    }

    return std::nullopt;
}

void NativeLog::setAppliedSequenceGetter(const StreamIDShard & stream_shard, std::function<int64_t()> applied_sn_getter)
{
    if (auto log = log_manager->getLog(stream_shard); log)
        log->setAppliedSequenceGetter(std::move(applied_sn_getter));
}

nlog::LogConfigPtr NativeLog::getLogConfig(const StreamIDShard & stream_shard) const
{
    if (!log_manager)
        return nullptr;

    return log_manager->getLogConfig(stream_shard);
}

size_t NativeLog::numStreamShards() const noexcept
{
    return log_manager ? log_manager->numStreamShards() : 0;
}
}
