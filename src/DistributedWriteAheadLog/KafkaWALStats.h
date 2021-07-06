#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

namespace Poco
{
    class Logger;
}

struct rd_kafka_s;

namespace DWAL
{
struct KafkaWALStatsSnap
{
    uint64_t received = 0;
    uint64_t dropped = 0;
    uint64_t failed = 0;
    uint64_t bytes = 0;

    std::string stat;
    std::string type;
};

struct KafkaWALStats
{
    explicit KafkaWALStats(Poco::Logger * log_, const std::string & type_) : log(log_), type(type_) { }

    std::string stats() const
    {
        std::lock_guard lock(stat_mutex);
        return stat;
    }

    KafkaWALStatsSnap snap() const
    {
        KafkaWALStatsSnap snap_stats;

        snap_stats.received = received;
        snap_stats.dropped = dropped;
        snap_stats.failed = failed;
        snap_stats.bytes = bytes;
        snap_stats.type = type;

        std::lock_guard lock(stat_mutex);
        snap_stats.stat = stat;

        return snap_stats;
    }

    static int logStats(struct rd_kafka_s * rk, char * json, size_t json_len, void * opaque)
    {
        auto * stats = static_cast<KafkaWALStats *>(opaque);
        return stats->logStats(rk, json, json_len);
    }

    static void logErr(struct rd_kafka_s * rk, int err, const char * reason, void * opaque)
    {
        auto * stats = static_cast<KafkaWALStats *>(opaque);
        stats->logErr(rk, err, reason);
    }

    static void logThrottle(struct rd_kafka_s * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * opaque)
    {
        auto * stats = static_cast<KafkaWALStats *>(opaque);
        stats->logThrottle(rk, broker_name, broker_id, throttle_time_ms);
    }

#if 0
    static void logOffsetCommits(struct rd_kafka_s * rk, rd_kafka_resp_err_t err, struct rd_kafka_topic_partition_list_s * offsets, void * opaque)
    {
        auto * stats = static_cast<KafkaWALStats *>(opaque);
        stats->logOffsetCommits(rk, err, offsets);
    }
#endif

private:
    int logStats(struct rd_kafka_s * rk, char * json, size_t json_len);

    void logErr(struct rd_kafka_s * rk, int err, const char * reason);

    void logThrottle(struct rd_kafka_s * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms);

    /// void logOffsetCommits(struct rd_kafka_s * rk, rd_kafka_resp_err_t err, struct rd_kafka_topic_partition_list_s * offsets);

public:
    Poco::Logger * log;
    std::string type;

    std::atomic_uint64_t received = 0;
    std::atomic_uint64_t dropped = 0;
    std::atomic_uint64_t failed = 0;
    std::atomic_uint64_t bytes = 0;

private:
    mutable std::mutex stat_mutex;
    /// JSON producer / consumer stats
    std::string stat;
};

using KafkaWALStatsPtr = std::unique_ptr<KafkaWALStats>;
}
