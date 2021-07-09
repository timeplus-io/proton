#pragma once

#include "KafkaWALSettings.h"
#include "KafkaWALStats.h"
#include "Results.h"

#include <Common/ThreadPool.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <atomic>

struct rd_kafka_s;
struct rd_kafka_topic_s;
struct rd_kafka_message_s;

namespace DWAL
{
struct KafkaWALContext;

/// KafkaWALSimpleConsumer consumes data from a specific single partition of a topic
/// It is designed on purpose that each SimpleConsumer will have dedicated thread
/// consuming the messages
/// The overall steps of consuming data by using this class are:
/// 1. init an instance `consumer` by calling ctor
/// 2. prepare KafkaWALContext ctx
/// 3. init topic handle as `consumer->initTopicHandle(ctx)`
/// 4. consume data by calling `consume` like `consumer->consume(..., ctx)`
/// 5. commit offset by calling `consumer->commit(..., ctx)`
/// 6. stop consumeing by calling `consumer->stopConsume(..., ctx)`
/// 7. dtor `consumer`
class KafkaWALSimpleConsumer final: private boost::noncopyable
{
public:
    explicit KafkaWALSimpleConsumer(std::unique_ptr<KafkaWALSettings> settings_);
    ~KafkaWALSimpleConsumer();

    void startup();
    void shutdown();

    /// `callback` will be invoked against the recrods for a partition of a topic
    /// The callback happens in the same thread as the caller
    int32_t consume(ConsumeCallback callback, void * data, const KafkaWALContext & ctx) const;

    ConsumeResult consume(uint32_t count, int32_t timeout_ms, const KafkaWALContext & ctx) const;

    /// Stop consuming for a partition of a topic
    int32_t stopConsume(const KafkaWALContext & ctx) const;

    /// Commit offset for a partition of a topic
    int32_t commit(int64_t offset, const KafkaWALContext & ctx) const;

    void initTopicHandle(KafkaWALContext & ctx) const;

private:
    /// Poll consume errors
    void backgroundPoll() const;

    void initHandle();

    int32_t startConsumingIfNotYet(const KafkaWALContext & ctx) const;

private:
    using FreeRdKafka = void (*)(struct rd_kafka_s *);
    using RdKafkaHandlePtr = std::unique_ptr<struct rd_kafka_s, FreeRdKafka>;

private:
    std::unique_ptr<KafkaWALSettings> settings;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    RdKafkaHandlePtr consumer_handle;

    ThreadPool poller;

    Poco::Logger * log;

    KafkaWALStatsPtr stats;
};
}
