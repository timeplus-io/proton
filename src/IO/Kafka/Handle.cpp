#include <IO/Kafka/Handle.h>

#include <IO/Kafka/mapErrorCode.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace DB
{

namespace Kafka
{

Handle::Handle(rd_kafka_type_t type, rd_kafka_conf_t * conf, int32_t poll_timeout_ms_)
    : poll_timeout_ms(poll_timeout_ms_), poller{CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 1}
{
    char errstr[512];
    rk.reset(rd_kafka_new(type, conf, errstr, sizeof(errstr)));
    /// librdkafka takes the ownership of the `rk_conf` object only if `rd_kafka_new` succeeds. So when it fails, we need to free it manually.
    if (!rk)
    {
        rd_kafka_conf_destroy(conf);
        throw Exception(mapErrorCode(rd_kafka_last_error()), "Failed to create kafka handle: {}", errstr);
    }

    logger = &Poco::Logger::get(getName());
}

Handle::~Handle()
{
    stopped.test_and_set();
    poller.wait();
}

void Handle::startPolling()
{
    std::call_once(poll_flag, [this]() {
        poller.scheduleOrThrowOnError([this] {
            LOG_INFO(logger, "Start polling");

            Stopwatch log_timer;
            while (!stopped.test())
            {
                auto n = rd_kafka_poll(get(), poll_timeout_ms);
                /// Log regularlly (but not too frequently) to show that polling is still working
                if (log_timer.elapsedMilliseconds() >= 300'000)
                {
                    LOG_INFO(logger, "Polled {} events", n);
                    log_timer.restart();
                }
            }

            LOG_INFO(logger, "Polling stopped");
        });
    });
}

ConsumerHandle::ConsumerHandle(const ConnectionPtr & owner_, rd_kafka_conf_t * conf) : Handle(RD_KAFKA_CONSUMER, conf), owner(owner_)
{
}

ConsumerPtr ConsumerHandle::consumeFrom(const std::string & topic)
{
    auto consumer = std::make_shared<Consumer>(owner, shared_from_this(), topic);
    markTopicConsumed(topic, consumer);
    return consumer;
}

void ConsumerHandle::markTopicConsumed(const std::string & topic, const ConsumerPtr & consumer)
{
    assert(!isConsuming(topic));
    std::lock_guard lock{consumers_mutex};
    consumers[topic] = consumer;
}

uint64_t ConsumerHandle::useCount()
{
    uint64_t count{0};

    std::lock_guard lock{consumers_mutex};
    for (const auto & [topic, ref] : consumers)
        if (!ref.expired())
            ++count;

    return count;
}

ProducerHandle::ProducerHandle(rd_kafka_conf_t * conf) : Handle(RD_KAFKA_PRODUCER, conf)
{
}

ProducerPtr ProducerHandle::produceTo(const std::string & topic)
{
    if (producers.contains(topic))
    {
        if (auto producer = producers[topic].lock())
        {
            LOG_INFO(logger, "Found producer on topic {}", topic);
            return producer;
        }
    }

    auto producer = std::make_shared<Producer>(shared_from_this(), topic);
    producers[topic] = producer;
    return producer;
}

}

}
