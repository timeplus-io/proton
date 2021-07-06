#include "KafkaWALStats.h"

#include <common/logger_useful.h>

#include <librdkafka/rdkafka.h>

namespace DWAL
{
int KafkaWALStats::logStats(struct rd_kafka_s * /* rk */, char * json, size_t json_len)
{
    std::string s(json, json + json_len);
    LOG_TRACE(log, "KafkaWALStats: {}", s);

    std::lock_guard lock(stat_mutex);
    stat.swap(s);
    return 0;
}

void KafkaWALStats::logErr(struct rd_kafka_s * rk, int err, const char * reason)
{
    failed += 1;

    if (err == RD_KAFKA_RESP_ERR__FATAL)
    {
        char errstr[512] = {'\0'};
        rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        LOG_ERROR(log, "Fatal error found, error={}", errstr);
    }
    else
    {
        LOG_WARNING(log, "Error occurred, error={}, reason={}", rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(err)), reason);
    }
}

void KafkaWALStats::logThrottle(struct rd_kafka_s * /* rk */, const char * broker_name, int32_t broker_id, int throttle_time_ms)
{
    LOG_WARNING(log, "Throttling occurred on broker={}, broker_id={}, throttle_time_ms={}", broker_name, broker_id, throttle_time_ms);
}

#if 0
void logOffsetCommits(struct rd_kafka_s * rk, rd_kafka_resp_err_t err, struct rd_kafka_topic_partition_list_s * offsets)
{
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR && err != RD_KAFKA_RESP_ERR__NO_OFFSET)
    {
        LOG_ERROR(stats->log, "Failed to commit offsets, error={}", rd_kafka_err2str(err));
    }

    for (int i = 0; offsets != nullptr && i < offsets->cnt; ++i)
    {
        rd_kafka_topic_partition_t * rktpar = &offsets->elems[i];
        LOG_INFO(
            stats->log,
            "Commits offsets, topic={} partition={} offset={} error={}",
            rktpar->topic,
            rktpar->partition,
            rktpar->offset,
            rd_kafka_err2str(err));
    }
}
#endif
}
