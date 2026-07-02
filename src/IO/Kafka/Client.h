#pragma once

#include <IO/Kafka/Common.h>
#include <IO/Kafka/Handle_fwd.h>
#include <Common/Logger.h>
#include <Common/SharedMutex.h>
#include <Common/Stopwatch.h>

#include <librdkafka/rdkafka.h>

#include <functional>
#include <unordered_map>

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
    const std::string & topicName() const;
    int32_t getPartitionCount(uint64_t timeout_ms) const;
    WatermarkOffsets queryWatermarkOffsets(int32_t partition, uint64_t timeout_ms) const;
    WatermarkOffsets getWatermarkOffsets(int32_t partition) const;

protected:
    using RdkTopicPtr = std::unique_ptr<rd_kafka_topic_t, decltype(rd_kafka_topic_destroy) *>;

    const std::string topic_name;

    std::shared_ptr<Handle> handle;
    RdkTopicPtr topic_handle;

    LoggerPtr logger;
};

class Consumer final : public Client, public std::enable_shared_from_this<Consumer>
{
public:
    Consumer(const ConnectionPtr &, const ConsumerHandlePtr &, const std::string & topic);

    using Callback = std::function<void(void * rkmessage, size_t total_count, void * data)>;
    using ErrorCallback = std::function<void(rd_kafka_resp_err_t errcode, std::string_view errmsg)>;

    /// Initialize the consumer for consuming data from the partitions.
    void initialize(const std::vector<uint64_t> & partitions);

    void startConsume(int32_t partition, int64_t offset);
    void stopConsume(int32_t partition);
    /// Fetch messages from the specified partition and process them with the callback.
    /// The error_callback will be called when receiving message fails.
    /// The callback will be called when all messages are received successfully (RD_KAFKA_RESP_ERR_NO_ERROR).
    void consumeBatch(int32_t partition, uint32_t count, int32_t timeout_ms, Callback callback, ErrorCallback error_callback);
    WatermarkOffsets getLastBatchOffsets(int32_t partition) const;
    std::pair<int64_t /* last_consumed_offset */, int64_t /* latest_offset */> getProgress(int32_t partition) const;

    /// Thread safe version of Client methods
    std::string name() const;
    int32_t getPartitionCount(uint64_t timeout_ms) const;
    WatermarkOffsets queryWatermarkOffsets(int32_t partition, uint64_t timeout_ms) const;
    WatermarkOffsets getWatermarkOffsets(int32_t partition) const;

    /// Recreates all the handles in the consumer.
    /// This is for solving the stall case: https://github.com/timeplus-io/proton-enterprise/issues/7519.
    ///
    /// The 'creation_time' is used to protect `recreate` function calls.
    /// Do not create new handle when the old one was created within certain time.
    /// A consumer could be shared by multiple sources, especially, when consuming a multi-parition topic,
    /// a KafkaSource will be created for each partition, and a Consumer instance is shared by all those sources.
    /// If stall happens, it's likely that it happens to all the sources that use the same Consumer. In this caee,
    /// they wil all call `recreate` at the (about) same time. If we don't protect `recreate`,
    /// then the Consumer will be recreated again and again for each source, which is disruptive and wasteful.
    bool recreate(UInt64 cooldown_ms);

    /// For internal use
    void updateFromWithLockHeld(Consumer && other);

private:
    void doStartConsume(int32_t partition, int64_t offset);
    void doStopConsume(int32_t partition);

    const ConnectionPtr owner;

    mutable SharedMutex mutex;
    Stopwatch creation_time;
    std::unordered_map<int32_t, WatermarkOffsets> partitions_progress;
};

using ConsumerPtr = std::shared_ptr<Consumer>;

class Producer final : public Client, public std::enable_shared_from_this<Producer>
{
public:
    Producer(const ProducerHandlePtr &, const std::string & topic);

    void start(bool need_poll = true);
    void stop();
};

using ProducerPtr = std::shared_ptr<Producer>;

}

}
