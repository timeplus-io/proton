#pragma once

#include <IO/Kafka/Common.h>
#include <IO/Kafka/Handle_fwd.h>
#include <Common/SharedMutex.h>

#include <librdkafka/rdkafka.h>
#include <Poco/Logger.h>

namespace DB
{

namespace Kafka
{

class Connection;
using ConnectionPtr = std::shared_ptr<Connection>;

/// Client is the base class of Consumer and Producer.
class Client
{
public:
    Client(const std::shared_ptr<Handle> &, const std::string & topic);
    virtual ~Client() = default;

    rd_kafka_t * getHandle() const;
    rd_kafka_topic_t * getTopicHandle() const;

    std::string name() const;
    std::string topicName() const;
    int32_t getPartitionCount() const;
    WatermarkOffsets queryWatermarkOffsets(int32_t partition) const;
    WatermarkOffsets getWatermarkOffsets(int32_t partition) const;

protected:
    using RdkTopicPtr = std::unique_ptr<rd_kafka_topic_t, decltype(rd_kafka_topic_destroy) *>;

    std::shared_ptr<Handle> handle;
    RdkTopicPtr topic_handle;

    Poco::Logger * logger;
};

class Consumer final : public Client, public std::enable_shared_from_this<Consumer>
{
public:
    Consumer(const ConnectionPtr &, const ConsumerHandlePtr &, const std::string & topic);

    using Callback = std::function<void(void * rkmessage, size_t total_count, void * data)>;
    using ErrorCallback = std::function<void(rd_kafka_resp_err_t)>;

    using Version = size_t;

    /// Initialize the consumer for consuming data from the partitions.
    void initialize(const std::vector<int32_t> & partitions);

    void startConsume(int32_t partition, int64_t offset);
    void stopConsume(int32_t partition);
    void consumeBatch(int32_t partition, uint32_t count, int32_t timeout_ms, Callback callback, ErrorCallback error_callback);
    WatermarkOffsets getLastBatchOffsets(int32_t partition) const;
    std::pair<int64_t /* last_consumed_offset */, int64_t /* latest_offset */> getProgress(int32_t partition) const;

    /// Recreates all the handles in the consumer.
    /// This is for solving the stall case: https://github.com/timeplus-io/proton-enterprise/issues/7519.
    ///
    /// The version is used to protect `recreate` function calls.
    /// A consumer could be shared by multiple sources, especially, when consuming a multi-parition topic,
    /// a KafkaSource will be created for each partition, and a Consumer instance is shared by all those sources.
    /// If stall happens, it's likely that it happens to all the sources that use the same Consumer. In this caee,
    /// they wil all call `recreate` at the (about) same time. If we don't protect `recreate`,
    /// then the Consumer will be recreated again and again for each source, which is disruptive and wasteful.
    /// With the version, we can make sure that only `recreate` called by the up-to-date version will actually recreate the Consumer.
    Version recreate(Version);
    Version getVersion() const { return ver; }

    /// For internal use
    void updateFrom(Consumer && other);

private:
    void doStartConsume(int32_t partition, int64_t offset);
    void doStopConsume(int32_t partition);

    const ConnectionPtr owner;

    mutable SharedMutex mutex;
    Version ver{0};
    std::unordered_map<int32_t, WatermarkOffsets> partitions_progress;
};

using ConsumerPtr = std::shared_ptr<Consumer>;

class Producer final : public Client, public std::enable_shared_from_this<Producer>
{
public:
    Producer(const ProducerHandlePtr &, const std::string & topic);

    void start();
    void stop();
};

using ProducerPtr = std::shared_ptr<Producer>;

}

}
