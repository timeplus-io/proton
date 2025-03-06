#pragma once

#include <IO/Kafka/Client.h>
#include <Common/ThreadPool.h>

#include <librdkafka/rdkafka.h>

namespace DB
{

namespace Kafka
{

class Connection;
using ConnectionPtr = std::shared_ptr<Connection>;

class Handle
{
public:
    /// Handle will take the ownership of the rd_kafka_conf_t pointer.
    Handle(rd_kafka_type_t, rd_kafka_conf_t *, int32_t poll_timeout_ms_ = 1000);
    virtual ~Handle();

    std::string getName() { return rd_kafka_name(rk.get()); }
    virtual bool isConsumer() const { return false; }

    rd_kafka_t * get() { return rk.get(); }

    void startPolling();

protected:
    Poco::Logger * logger;

private:
    using RdKafkaPtr = std::unique_ptr<rd_kafka_t, decltype(rd_kafka_destroy) *>;

    RdKafkaPtr rk{nullptr, rd_kafka_destroy};

    int32_t poll_timeout_ms{0};
    std::once_flag poll_flag;
    std::atomic_flag stopped;
    ThreadPool poller;
};

class ConsumerHandle : public Handle, public std::enable_shared_from_this<ConsumerHandle>
{
public:
    explicit ConsumerHandle(const ConnectionPtr &, rd_kafka_conf_t *);

    bool isConsumer() const override { return true; }

    /// Creates a consumer with current handle for topic.
    ConsumerPtr consumeFrom(const std::string & topic);
    /// Marks the topic is being consumed by the consumer.
    void markTopicConsumed(const std::string & topic, const ConsumerPtr & consumer);
    inline bool isConsuming(const std::string & topic)
    {
        std::lock_guard lock{consumers_mutex};
        auto iter = consumers.find(topic);
        return iter != consumers.end() && !iter->second.expired();
    }
    uint64_t useCount();

private:
    const ConnectionPtr owner;
    std::mutex consumers_mutex;
    std::unordered_map<std::string, std::weak_ptr<Consumer>> consumers;
};

using ConsumerHandlePtr = std::shared_ptr<ConsumerHandle>;

class ProducerHandle final : public Handle, public std::enable_shared_from_this<ProducerHandle>
{
    friend Producer;

public:
    explicit ProducerHandle(rd_kafka_conf_t *);

    /// Create a producer for producing messages to the topic.
    /// If there is already producer created for that same topic, just return that existing producer.
    ProducerPtr produceTo(const std::string & topic);

    uint64_t useCount() const { return use_count; }

private:
    void increaseUseCount() { ++use_count; }
    void decreaseUseCount() { --use_count; }

    std::unordered_map<std::string, std::weak_ptr<Producer>> producers;
    std::atomic_uint64_t use_count{0};
};

using ProducerHandlePtr = std::shared_ptr<ProducerHandle>;

}

}
