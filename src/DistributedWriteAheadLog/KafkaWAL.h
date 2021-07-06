#pragma once

#include "KafkaWALContext.h"
#include "KafkaWALSettings.h"
#include "KafkaWALSimpleConsumer.h"
#include "KafkaWALStats.h"
#include "Results.h"
#include "WAL.h"

#include <Common/ThreadPool.h>


struct rd_kafka_s;
struct rd_kafka_topic_s;
struct rd_kafka_message_s;

namespace DWAL
{
class KafkaWAL final : public WAL
{
public:
    explicit KafkaWAL(std::unique_ptr<KafkaWALSettings> settings_);
    ~KafkaWAL() override;

    void startup() override;
    void shutdown() override;
    std::string type() const override { return "kafka"; }

    ClusterPtr cluster(std::any & ctx) const override;

    /// `ctx` is KafkaWALContext
    AppendResult append(const Record & record, std::any & ctx) override;

    /// Async append, we don't poll result but rely on callback to deliver the append result back
    /// The AppendCallback can be called in a different thread, so caller need make sure its
    /// multi-thread safety and lifetime validity. Same for the `data` passed in.
    /// `ctx` is KafkaWALContext
    int32_t append(const Record & record, AppendCallback callback, void * data, std::any & ctx) override;

    /// Poll the async `append` status
    void poll(int32_t timeout_ms, std::any & ctx) override;

    /// `ctx` is KafkaWALContext
    /// register a consumer callback for topic, partition
    int32_t consume(ConsumeCallback callback, void * data, std::any & ctx) override;

    /// `ctx` is KafkaWALContext
    ConsumeResult consume(uint32_t count, int32_t timeout_ms, std::any & ctx) override;

    /// `ctx` is KafkaWALContext
    /// Stop consuming
    int32_t stopConsume(std::any & ctx) override;

    /// `ctx` is KafkaWALContext
    /// `commit` doesn't really commit offsets to Kafka brokers instead it stores offsets in
    /// memory and will be later committed in batch (every `auto_commit_internval_ms`). It doesn't
    /// commit offset synchronously because of performance concerns
    int32_t commit(RecordSN sn, std::any & ctx) override;

    /// APIs for clients to cache the topic handle
    std::shared_ptr<rd_kafka_topic_s> initProducerTopicHandle(const KafkaWALContext & ctx);

    /// Admin APIs
    /// `ctx` is KafkaWALContext
    int32_t create(const std::string & name, std::any & ctx) override;

    /// `ctx` is KafkaWALContext
    int32_t remove(const std::string & name, std::any & ctx) override;

    /// `ctx` is KafkaWALContext
    int32_t describe(const std::string & name, std::any & ctx) const override;

private:
    using FreeRdKafka = void (*)(struct rd_kafka_s *);
    using RdKafkaHandlePtr = std::unique_ptr<struct rd_kafka_s, FreeRdKafka>;

    struct DeliveryReport
    {
        std::atomic_int32_t partition = -1;
        std::atomic_int64_t offset = -1;
        std::atomic_int32_t err = 0;
        AppendCallback callback = nullptr;
        void * data = nullptr;
        bool delete_self = false;

        explicit DeliveryReport(AppendCallback callback_ = nullptr, void * data_ = nullptr, bool delete_self_ = false)
            : callback(callback_), data(data_), delete_self(delete_self_)
        {
        }
    };

private:
    void initProducerHandle();

    /// Poll delivery report
    void backgroundPollProducer();

    AppendResult handleError(int err, const Record & record, const KafkaWALContext & ctx);

    /// DeliveryReport `dr` must reside on heap. if `doAppend` succeeds, the DeliveryReport object is handled over
    /// to librdkafka and will be used by `delivery_report` callback eventually
    int32_t doAppend(const Record & record, DeliveryReport * dr, KafkaWALContext & walctx);

private:
    static void deliveryReport(struct rd_kafka_s *, const rd_kafka_message_s * rkmessage, void * /*opaque*/);

private:
    std::unique_ptr<KafkaWALSettings> settings;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    RdKafkaHandlePtr producer_handle;
    std::unique_ptr<KafkaWALSimpleConsumer> consumer;

    ThreadPool poller;

    Poco::Logger * log;

    KafkaWALStatsPtr stats;
};
}
