#include "KafkaWALSimpleConsumer.h"
#include "KafkaWALCommon.h"
#include "KafkaWALContext.h"

#include <Common/setThreadName.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int RESOURCE_NOT_INITED;
}
}

namespace DWAL
{
KafkaWALSimpleConsumer::KafkaWALSimpleConsumer(std::unique_ptr<KafkaWALSettings> settings_)
    : settings(std::move(settings_))
    , consumer_handle(nullptr, rd_kafka_destroy)
    , poller(1)
    , log(&Poco::Logger::get("KafkaWALSimpleConsumer"))
    , stats{std::make_unique<KafkaWALStats>(log, "simple_consumer")}
{
}

KafkaWALSimpleConsumer::~KafkaWALSimpleConsumer()
{
    shutdown();
}

void KafkaWALSimpleConsumer::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    initHandle();

    poller.scheduleOrThrowOnError([this] { backgroundPoll(); });

    LOG_INFO(log, "Started");
}

void KafkaWALSimpleConsumer::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");
    poller.wait();
    LOG_INFO(log, "Stopped");
}

void KafkaWALSimpleConsumer::initHandle()
{
    /// 1) use simple Kafka consumer
    /// 2) manually manage offset commit
    /// 3) offsets are stored in brokers and on application side.
    ///     3.1) In normal cases, application side offsets overrides offsets in borkers
    ///     3.2) In corruption cases (application offsets corruption), use offsets in borkers
    /// https://github.com/edenhill/librdkafka/wiki/Consumer-offset-management
    std::vector<std::pair<String, String>> consumer_params = {
        std::make_pair("bootstrap.servers", settings->brokers.c_str()),
        std::make_pair("group.id", settings->group_id),
        /// Enable auto offset commit
        std::make_pair("enable.auto.commit", "true"),
        std::make_pair("auto.commit.interval.ms", std::to_string(settings->auto_commit_interval_ms)),
        std::make_pair("fetch.message.max.bytes", std::to_string(settings->fetch_message_max_bytes)),
        std::make_pair("fetch.wait.max.ms", std::to_string(settings->fetch_wait_max_ms)),
        /// Disable librdkafka committing offset prior handling messages to applications
        std::make_pair("enable.auto.offset.store", "false"),
        /// By default offset.store.method is broker. Enabling it gives a warning message
        /// https://github.com/edenhill/librdkafka/pull/3035
        /// std::make_pair("offset.store.method", "broker"),
        std::make_pair("enable.partition.eof", "false"),
        std::make_pair("queued.min.messages", std::to_string(settings->queued_min_messages)),
        std::make_pair("queued.max.messages.kbytes", std::to_string(settings->queued_max_messages_kbytes)),
    };

    if (!settings->debug.empty())
    {
        consumer_params.emplace_back("debug", settings->debug);
    }

    auto cb_setup = [](rd_kafka_conf_t * kconf) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_conf_set_stats_cb(kconf, &KafkaWALStats::logStats);
        rd_kafka_conf_set_error_cb(kconf, &KafkaWALStats::logErr);
        rd_kafka_conf_set_throttle_cb(kconf, &KafkaWALStats::logThrottle);

        /// Consumer offset commits
        /// rd_kafka_conf_set_offset_commit_cb(kconf, &KafkaWALStats::logOffsetCommits);
    };

    consumer_handle = initRdKafkaHandle(RD_KAFKA_CONSUMER, consumer_params, stats.get(), cb_setup);

    /// Forward all events to consumer queue. there may have in-balance consuming problems
    /// rd_kafka_poll_set_consumer(consumer_handle.get());
}

void KafkaWALSimpleConsumer::backgroundPoll()
{
    LOG_INFO(log, "Polling consumer started");
    setThreadName("KWalCPoller");

    while (!stopped.test())
    {
        rd_kafka_poll(consumer_handle.get(), 100);
    }

    auto err = rd_kafka_commit(consumer_handle.get(), nullptr, 0);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR && err != RD_KAFKA_RESP_ERR__NO_OFFSET)
    {
        LOG_ERROR(log, "Failed to commit offsets, error={}", rd_kafka_err2str(err));
    }

    LOG_INFO(log, "Polling consumer stopped");
}

std::shared_ptr<rd_kafka_topic_s> KafkaWALSimpleConsumer::initTopicHandle(const KafkaWALContext & ctx)
{
    assert(inited.test());

    KConfParams topic_params = {
        /// enable auto offset commit
        std::make_pair("enable.auto.commit", "true"), /// LEGACY topic settings
        std::make_pair("auto.commit.interval.ms", std::to_string(settings->auto_commit_interval_ms)), /// LEGACY topic settings
        std::make_pair("auto.offset.reset", ctx.auto_offset_reset),
        std::make_pair("consume.callback.max.messages", std::to_string(ctx.consume_callback_max_messages)),
    };

    return initRdKafkaTopicHandle(ctx.topic, topic_params, consumer_handle.get(), stats.get());
}

inline int32_t KafkaWALSimpleConsumer::initTopicHandleIfNecessary(KafkaWALContext & ctx)
{
    if (unlikely(!ctx.topic_handle))
    {
        ctx.topic_handle = initTopicHandle(ctx);
        /// Always starts from broker stored offset. Since for simple consumer, if we specify a
        // positive offset manually, it will disable auto-commit.
        /// We will filter uneeded messages according to ctx.offset in consume function
        if (rd_kafka_consume_start(ctx.topic_handle.get(), ctx.partition, RD_KAFKA_OFFSET_STORED) == -1)
        {
            LOG_ERROR(
                log,
                "Failed to start consuming topic={} partition={} offset={} error={}",
                ctx.topic,
                ctx.partition,
                ctx.offset,
                rd_kafka_err2str(rd_kafka_last_error()));

            return mapErrorCode(rd_kafka_last_error());
        }
    }

    return DB::ErrorCodes::OK;
}

int32_t KafkaWALSimpleConsumer::consume(ConsumeCallback callback, void * data, KafkaWALContext & ctx)
{
    auto err = initTopicHandleIfNecessary(ctx);
    if (err != 0)
    {
        return err;
    }

    struct WrappedData
    {
        ConsumeCallback callback;
        void * data;

        RecordPtrs records;
        KafkaWALContext & ctx;
        Poco::Logger * log;

        Int64 current_size = 0;
        Int64 current_rows = 0;

        WrappedData(
            ConsumeCallback callback_,
            void * data_,
            KafkaWALContext & ctx_,
            Poco::Logger * log_)
            : callback(callback_), data(data_), ctx(ctx_), log(log_)
        {
            records.reserve(100);
        }
    };

    WrappedData wrapped_data{callback, data, ctx, log};

    auto kcallback = [](rd_kafka_message_t * rkmessage, void * kdata) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        auto wrapped = static_cast<WrappedData *>(kdata);

        if (likely(rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR))
        {
            if (rkmessage->offset < wrapped->ctx.offset)
            {
                /// Ignore the message which has lower offset than what clients like to have
                return;
            }

            auto record = kafkaMsgToRecord(rkmessage);
            if (likely(record))
            {
                wrapped->current_size += record->block.bytes();
                wrapped->current_rows += record->block.rows();
                wrapped->records.push_back(std::move(record));
                assert(!record);
            }
            else
            {
                LOG_WARNING(
                    wrapped->log,
                    "Returns nullptr record when consuming topic={} partition={}",
                    wrapped->ctx.topic,
                    wrapped->ctx.partition);
            }

            if (wrapped->current_size >= wrapped->ctx.consume_callback_max_messages_size || wrapped->current_rows >= wrapped->ctx.consume_callback_max_rows)
            {
                if (likely(wrapped->callback))
                {
                    try
                    {
                        auto size = wrapped->records.size();
                        wrapped->callback(std::move(wrapped->records), wrapped->data);
                        assert(wrapped->records.empty());
                        wrapped->records.reserve(size);
                        wrapped->current_size = 0;
                        wrapped->current_rows = 0;
                    }
                    catch (...)
                    {
                        LOG_ERROR(
                            wrapped->log,
                            "Failed to consume topic={} partition={} error={}",
                            wrapped->ctx.topic,
                            wrapped->ctx.partition,
                            DB::getCurrentExceptionMessage(true, true));
                        throw;
                    }
                }
            }
        }
        else
        {
            LOG_ERROR(
                wrapped->log,
                "Failed to consume topic={} partition={} error={}",
                wrapped->ctx.topic,
                wrapped->ctx.partition,
                rd_kafka_message_errstr(rkmessage));
        }
    };

    if (rd_kafka_consume_callback(ctx.topic_handle.get(), ctx.partition, ctx.consume_callback_timeout_ms, kcallback, &wrapped_data)
        == -1)
    {
        LOG_ERROR(
            log,
            "Failed to consume topic={} partition={} offset={} error={}",
            ctx.topic,
            ctx.partition,
            ctx.offset,
            rd_kafka_err2str(rd_kafka_last_error()));
        return mapErrorCode(rd_kafka_last_error());
    }

    /// The last batch
    if (!wrapped_data.records.empty())
    {
        if (likely(callback))
        {
            try
            {
                callback(std::move(wrapped_data.records), data);
                assert(wrapped_data.records.empty());
            }
            catch (...)
            {
                LOG_ERROR(
                    log,
                    "Failed to consume topic={} partition={} error={}",
                    ctx.topic,
                    ctx.partition,
                    DB::getCurrentExceptionMessage(true, true));
                throw;
            }
        }
    }

    return DB::ErrorCodes::OK;
}

ConsumeResult KafkaWALSimpleConsumer::consume(uint32_t count, int32_t timeout_ms, KafkaWALContext & ctx)
{
    auto err = initTopicHandleIfNecessary(ctx);
    if (err != 0)
    {
        return {.err = mapErrorCode(rd_kafka_last_error()), .records = {}};
    }

    std::unique_ptr<rd_kafka_message_t *, decltype(free) *> rkmessages{
        static_cast<rd_kafka_message_t **>(malloc(sizeof(*rkmessages) * count)), free};
    auto res = rd_kafka_consume_batch(ctx.topic_handle.get(), ctx.partition, timeout_ms, rkmessages.get(), count);

    if (res >= 0)
    {
        ConsumeResult result;
        result.records.reserve(res);

        for (ssize_t idx = 0; idx < res; ++idx)
        {
            auto rkmessage = rkmessages.get()[idx];
            if (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                if (rkmessage->offset < ctx.offset)
                {
                    /// Ignore the message which has lower offset than what clients like to have
                    continue;
                }

                auto record = kafkaMsgToRecord(rkmessage);
                if (likely(record))
                {
                    result.records.push_back(record);
                }
                else
                {
                    LOG_WARNING(
                        log, "Returns nullptr record when consuming topic={} partition={}", ctx.topic, ctx.partition);
                }
            }
            else
            {
                LOG_ERROR(
                    log,
                    "Failed to consume topic={} partition={} error={}",
                    ctx.topic,
                    ctx.partition,
                    rd_kafka_message_errstr(rkmessage));
            }

            rd_kafka_message_destroy(rkmessages.get()[idx]);
        }
        return result;
    }
    else
    {
        LOG_ERROR(
            log, "Failed to consuming topic={} partition={} error={}", ctx.topic, ctx.partition, rd_kafka_err2str(rd_kafka_last_error()));

        return {.err = mapErrorCode(rd_kafka_last_error()), .records = {}};
    }
}

int32_t KafkaWALSimpleConsumer::stopConsume(KafkaWALContext & ctx)
{
    if (!ctx.topic_handle)
    {
        LOG_ERROR(log, "Didn't start consuming topic={} partition={} yet", ctx.topic, ctx.partition);
        return DB::ErrorCodes::RESOURCE_NOT_INITED;
    }

    if (rd_kafka_consume_stop(ctx.topic_handle.get(), ctx.partition) == -1)
    {
        LOG_ERROR(
            log,
            "Failed to stop consuming topic={} partition={} error={}",
            ctx.topic,
            ctx.partition,
            rd_kafka_err2str(rd_kafka_last_error()));

        return mapErrorCode(rd_kafka_last_error());
    }

    return DB::ErrorCodes::OK;
}

int32_t KafkaWALSimpleConsumer::commit(int64_t offset, KafkaWALContext & ctx)
{
    if (!ctx.topic_handle)
    {
        ctx.topic_handle = initTopicHandle(ctx);
    }

    LOG_INFO(log, "Stores commit offset={} for topic={} partition={}", offset, ctx.topic, ctx.partition);

    auto err = rd_kafka_offset_store(ctx.topic_handle.get(), ctx.partition, offset);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(
            log,
            "Failed to commit offset={} for topic={} partition={} error={}",
            offset,
            ctx.topic,
            ctx.partition,
            rd_kafka_err2str(err));
        return mapErrorCode(err);
    }

    return DB::ErrorCodes::OK;
}
}
