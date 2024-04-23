#pragma once

#include <Common/logger_useful.h>
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
    Consumer(const rd_kafka_conf_t & rk_conf, UInt64 poll_timeout_ms, const String & logger_name_prefix);
    ~Consumer();

    rd_kafka_t * getHandle() const { return rk.get(); }

    std::vector<Int64> getOffsetsForTimestamps(const std::string & topic, const std::vector<klog::PartitionTimestamp> & partition_timestamps, int32_t timeout_ms = 5000) const;

    void startConsume(Topic & topic, Int32 parition, Int64 offset = RD_KAFKA_OFFSET_END);
    void stopConsume(Topic & topic, Int32 parition);

    using Callback = std::function<void(void * rkmessage, size_t total_count, void * data)>;
    using ErrorCallback = std::function<void(rd_kafka_resp_err_t)>;

    void consumeBatch(Topic & topic, Int32 partition, uint32_t count, int32_t timeout_ms, Callback callback, ErrorCallback error_callback) const;

    void setStopped() {
        stopped.test_and_set();
        LOG_INFO(logger, "Stopped");
    }

    bool isStopped() const { return stopped.test(); }

    std::string name() const { return rd_kafka_name(rk.get()); }

private:
    void backgroundPoll() const;

    UInt64 poll_timeout_ms {0};
    klog::KafkaPtr rk {nullptr, rd_kafka_destroy};
    ThreadPool poller;
    Poco::Logger * logger;

    std::atomic_flag started;
    std::atomic_flag stopped;
};

}

}
