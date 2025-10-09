#pragma once

#include <Checkpoint/ExtraCheckpointContext.h>

#include <Cluster/Common/Constants.h>
#include <Cluster/Common/StreamShard.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>

#include <Poco/Logger.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace DB
{
struct LogStoreCheckpointContext final : public ExtraCheckpointContext
{
    std::function<bool(Int64)> stopped;


    const cluster::StreamShard log_stream_shard;
    const size_t max_entry_size;

    std::atomic<Int64> next_sn = cluster::Constants::LatestSN;
    std::atomic<Int64> applied_sn = 0;

    std::atomic<uint16_t> keys_count_of_current_epoch = 0;
    LoggerPtr logger = nullptr;

    std::atomic_bool heavy_state_hint = false;

    std::atomic_bool force_request_full_ckpt = false;

    std::mutex mutex; /// Lock to write to the log store

    LogStoreCheckpointContext(
        std::function<bool(Int64)> materializing_stopped_,
        const cluster::StreamShard & log_stream_shard_,
        size_t max_entry_size_,
        bool heavy_state_hint_ = false)
        : stopped(std::move(materializing_stopped_))
        , log_stream_shard(log_stream_shard_)
        , max_entry_size(max_entry_size_)
        , logger(getLogger(fmt::format("{}.ckpt", log_stream_shard.logName())))
        , heavy_state_hint(heavy_state_hint_)
    {
        assert(stopped);
    }

    ~LogStoreCheckpointContext() override = default;
};

using LogStoreCheckpointContextPtr = std::shared_ptr<LogStoreCheckpointContext>;
}
