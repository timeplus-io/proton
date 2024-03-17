#pragma once

#include <Common/ThreadPool.h>
#include <KafkaLog/KafkaWALCommon.h>
#include <Storages/ExternalStream/Kafka/Topic.h>

#include <boost/core/noncopyable.hpp>

namespace DB
{

namespace RdKafka
{

class Consumer : boost::noncopyable
{
public:
    Consumer(rd_kafka_conf_t * rk_conf, Poco::Logger * logger_);
    ~Consumer()
    {
        stopped.test_and_set();
    }

    rd_kafka_t * getHandle() const { return rk.get(); }

    /// Fetches the number of partitions of the topic by describing the topic.
    int describeTopic(const std::string & topic_name) const;

    std::vector<Int64> getOffsetsForTimestamps(const std::string & topic, const std::vector<klog::PartitionTimestamp> & partition_timestamps, int32_t timeout_ms = 5000) const;

    void startConsume(Topic & topic, Int32 parition, Int64 offset = RD_KAFKA_OFFSET_END);
    void stopConsume(Topic & topic, Int32 parition);

    using Callback = std::function<void(void * rkmessage, size_t total_count, void * data)>;
    using ErrorCallback = std::function<void(rd_kafka_resp_err_t)>;

    void consumeBatch(Topic & topic, Int32 partition, uint32_t count, int32_t timeout_ms, Callback callback, ErrorCallback error_callback) const;

private:
    std::string name() const { return rd_kafka_name(rk.get()); }
    void backgroundPoll() const;

    klog::KafkaPtr rk {nullptr, rd_kafka_destroy};
    ThreadPool poller;
    std::atomic_flag stopped;
    Poco::Logger * logger;
};

}

}
