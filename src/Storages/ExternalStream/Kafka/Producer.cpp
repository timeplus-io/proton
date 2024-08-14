#include <Common/logger_useful.h>
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
Producer::Producer(const rd_kafka_conf_t & rk_conf, UInt64 poll_timeout_ms, const String & logger_name_prefix)
{
    char errstr[512];
    auto * conf = rd_kafka_conf_dup(&rk_conf);
    rk.reset(rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)));
    if (!rk)
    {
        /// librdkafka only take the ownership of `rk_conf` if `rd_kafka_new` succeeds,
        /// we need to free it otherwise.
        rd_kafka_conf_destroy(conf);
        throw Exception(klog::mapErrorCode(rd_kafka_last_error()), "Failed to create kafka handle: {}", errstr);
    }

    logger = &Poco::Logger::get(fmt::format("{}.{}", logger_name_prefix, name()));
    LOG_INFO(logger, "Created producer");

    poller.scheduleOrThrowOnError([this, poll_timeout_ms] { backgroundPoll(poll_timeout_ms); });
}

Producer::~Producer() {
    setStopped();
    poller.wait();
}

void Producer::backgroundPoll(UInt64 poll_timeout_ms) const
{
    LOG_INFO(logger, "Start producer poll");

    while (!stopped.test())
        rd_kafka_poll(rk.get(), poll_timeout_ms);

    LOG_INFO(logger, "Producer poll stopped");
}

}

}
