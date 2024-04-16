#include <Poco/Logger.h>
#include <Storages/ExternalStream/Kafka/Consumer.h>

namespace DB
{

namespace RdKafka
{

/// Consumer will take the ownership of `rk_conf`.
Consumer::Consumer(const rd_kafka_conf_t & rk_conf, UInt64 poll_timeout_ms_, const String & logger_name_prefix)
: poll_timeout_ms(poll_timeout_ms_)
{
    char errstr[512];
    auto * conf = rd_kafka_conf_dup(&rk_conf);
    rk.reset(rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr)));
    if (!rk)
    {
        /// librdkafka only take the ownership of `rk_conf` if `rd_kafka_new` succeeds,
        /// we need to free it otherwise.
        rd_kafka_conf_destroy(conf);
        throw Exception(klog::mapErrorCode(rd_kafka_last_error()), "Failed to create kafka handle: {}", errstr);
    }

    logger = &Poco::Logger::get(fmt::format("{}.{}", logger_name_prefix, name()));
    LOG_INFO(logger, "Created consumer");

    poller.scheduleOrThrowOnError([this] { backgroundPoll(); });
}

Consumer::~Consumer()
{
    setStopped();
    poller.wait();
}

void Consumer::backgroundPoll() const
{
    LOG_INFO(logger, "Start consumer poll");

    while (!stopped.test())
        rd_kafka_poll(rk.get(), poll_timeout_ms);

    LOG_INFO(logger, "Consumer poll stopped");
}

std::vector<Int64> Consumer::getOffsetsForTimestamps(const std::string & topic, const std::vector<klog::PartitionTimestamp> & partition_timestamps, int32_t timeout_ms) const
{
    return klog::getOffsetsForTimestamps(rk.get(), topic, partition_timestamps, timeout_ms);
}

void Consumer::startConsume(Topic & topic, Int32 parition, Int64 offset)
{
    auto res = rd_kafka_consume_start(topic.getHandle(), parition, offset);
    if (res == -1)
    {
        auto err = rd_kafka_last_error();
        throw Exception(klog::mapErrorCode(err), "Failed to start consuming topic={} parition={} offset={} error={}", topic.name(), parition, offset, rd_kafka_err2str(err));
    }
}

void Consumer::stopConsume(Topic & topic, Int32 parition)
{
    auto res = rd_kafka_consume_stop(topic.getHandle(), parition);
    if (res == -1)
    {
        auto err = rd_kafka_last_error();
        throw Exception(klog::mapErrorCode(err), "Failed to stop consuming topic={} parition={} error={}", topic.name(), parition, rd_kafka_err2str(err));
    }
}

void Consumer::consumeBatch(Topic & topic, Int32 partition, uint32_t count, int32_t timeout_ms, Consumer::Callback callback, ErrorCallback error_callback) const
{
    std::unique_ptr<rd_kafka_message_t *, decltype(free) *> rkmessages
    {
        static_cast<rd_kafka_message_t **>(malloc(sizeof(rd_kafka_message_t *) * count)), free
    };

    auto res = rd_kafka_consume_batch(topic.getHandle(), partition, timeout_ms, rkmessages.get(), count);

    if (res < 0)
    {
        error_callback(rd_kafka_last_error());
        return;
    }

    for (ssize_t idx = 0; idx < res; ++idx)
    {
        auto * rkmessage = rkmessages.get()[idx];
        try
        {
            if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR)
                error_callback(rkmessage->err);
            else
                callback(rkmessage, res, nullptr);
        }
        catch (...)
        {
            /// just log the error to make sure the messages get destroyed
            LOG_ERROR(
                logger,
                "Uncaught exception during consuming topic={} partition={} error={}",
                topic.name(), partition, DB::getCurrentExceptionMessage(true, true));
        }

        rd_kafka_message_destroy(rkmessage);
    }
}

}

}
