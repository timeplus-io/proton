#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Storages/ExternalStream/Kafka/Producer.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
extern const int INVALID_SETTING_VALUE;
}

namespace RdKafka
{

/// Producer will take the ownership of `rk_conf`.
Producer::Producer(rd_kafka_conf_t * rk_conf, UInt64 poll_timeout_ms, Poco::Logger * logger_) : logger(logger_)
{
    char errstr[512];
    rk.reset(rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, errstr, sizeof(errstr)));
    if (!rk)
    {
        /// librdkafka only take the ownership of `rk_conf` if `rd_kafka_new` succeeds,
        /// we need to free it otherwise.
        rd_kafka_conf_destroy(rk_conf);
        throw Exception(klog::mapErrorCode(rd_kafka_last_error()), "Failed to create kafka handle: {}", errstr);
    }

    poller.scheduleOrThrowOnError([this, poll_timeout_ms] { backgroundPoll(poll_timeout_ms); });
}

void Producer::backgroundPoll(UInt64 poll_timeout_ms) const
{
    // setThreadName((name() + "-producer-poll").data());
    LOG_INFO(logger, "Start producer poll");

    while (!stopped.test())
        rd_kafka_poll(rk.get(), poll_timeout_ms);

    LOG_INFO(logger, "Producer poll stopped");
}

}

}
