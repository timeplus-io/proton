#pragma once

#include "KafkaWALCluster.h"
#include "KafkaWALContext.h"
#include "KafkaWALSettings.h"
#include "KafkaWALSimpleConsumer.h"
#include "KafkaWALStats.h"
#include "Results.h"

#include <Common/ThreadPool.h>


struct rd_kafka_s;
struct rd_kafka_topic_s;
struct rd_kafka_message_s;

namespace DWAL
{
/** Kafka as a Distributed Write Ahead Log (WAL) interfaces which defines an ordered sequence of `transitions`.
 * At its core, it is an sequntial orderded and append-only log abstraction
 * It generally can store any `transition` operation including but not limited by the following ones,
 * as long as the operation can wrap in a `Block` and can be understood in the all partiticipants invovled.
 * 1. Insert a data block (data path)
 * 2. Mutate commands like `ALTER TABLE table UPDATE ...`
 * 3. DDL commands like `CREATE TABLE table ...`
 * 4. ...
 */

/// The overall steps of CRUD a topic by using this calss are:
/// 1. init an instance `wal` by calling ctor
/// 2. parepare KafkaWALContext ctx
/// 3. call wal->describe(..., ctx) to check if the topic exists
/// 4. call wal->create(..., ctx) to create a topic according to params in ctx
/// 5. call wal->remove(..., ctx) to delete the topic

/// The overall steps of producing data by using this class are:
/// 1. init an instance `wal` by calling ctor
/// 2. prepare KafkaWALContext ctx
/// 3. init produce topic handle as `wal->initProducerTopicHandle(ctx)`
/// 4. produce data by calling `append` like `wal->append(..., ctx)`
/// 5. dtor `wal`

/// The overall steps of consuming data by using this class are:
/// 1. init an instance `wal` by calling ctor
/// 2. prepare KafkaWALContext ctx
/// 3. init consumer topic handle as `wal->initConsumerTopicHandle(ctx)`
/// 4. consume data by calling `consume` like `wal->consume(..., ctx)`
/// 5. commit offset by calling `wal->commit(..., ctx)`
/// 6. stop consumeing by calling `wal->stopConsume(..., ctx)`
/// 7. dtor `wal`

class KafkaWAL final
{
public:
    explicit KafkaWAL(std::unique_ptr<KafkaWALSettings> settings_);
    ~KafkaWAL();

    void startup();
    void shutdown();

    AppendResult append(const Record & record, const KafkaWALContext & ctx) const;

    /// Async append, we don't poll result but rely on callback to deliver the append result back
    /// The AppendCallback can be called in a different thread, so caller need make sure its
    /// multi-thread safety and lifetime validity. Same for the `data` passed in.
    int32_t append(const Record & record, AppendCallback callback, void * data, const KafkaWALContext & ctx) const;

    /// Poll the async `append` status
    void poll(int32_t timeout_ms, const KafkaWALContext & ctx) const;

    /// register a consumer callback for topic, partition
    int32_t consume(ConsumeCallback callback, void * data, const KafkaWALContext & ctx) const;

    ConsumeResult consume(uint32_t count, int32_t timeout_ms, const KafkaWALContext & ctx) const;

    int32_t stopConsume(const KafkaWALContext & ctx) const;

    /// `commit` doesn't really commit offsets to Kafka brokers instead it stores offsets in
    /// memory and will be later committed in batch (every `auto_commit_internval_ms`). It doesn't
    /// commit offset synchronously because of performance concerns
    int32_t commit(RecordSN sn, const KafkaWALContext & ctx) const;

    /// Init producer topic handle in ctx. When fail, throw exception
    void initProducerTopicHandle(KafkaWALContext & ctx) const;

    /// Init consumer topic handle in ctx. When fail, throw exception
    void initConsumerTopicHandle(KafkaWALContext & ctx) const;

    /// Admin APIs
    int32_t create(const std::string & name, const KafkaWALContext & ctx) const;

    int32_t remove(const std::string & name, const KafkaWALContext & ctx) const;

    DescribeResult describe(const std::string & name, const KafkaWALContext & ctx) const;

    KafkaWALClusterPtr cluster(const KafkaWALContext & ctx) const;

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
    void backgroundPollProducer() const;

    AppendResult handleError(int err, const Record & record, const KafkaWALContext & ctx) const;

    /// DeliveryReport `dr` must reside on heap. if `doAppend` succeeds, the DeliveryReport object is handled over
    /// to librdkafka and will be used by `delivery_report` callback eventually
    int32_t doAppend(const Record & record, DeliveryReport * dr, const KafkaWALContext & walctx) const;

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

using KafkaWALPtr = std::shared_ptr<KafkaWAL>;
using KafkaWALPtrs = std::vector<KafkaWALPtr>;
}
