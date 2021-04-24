#include "DistributedWriteAheadLogKafka.h"

#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>

#include <librdkafka/rdkafka.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int RESOURCE_NOT_FOUND;
    extern const int RESOURCE_NOT_INITED;
    extern const int RESOURCE_ALREADY_EXISTS;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_EXCEPTION;
    extern const int BAD_ARGUMENTS;
    extern const int DWAL_FATAL_ERROR;
}

namespace
{
/// types
using KConfPtr = std::unique_ptr<rd_kafka_conf_t, decltype(rd_kafka_conf_destroy) *>;
using KTopicConfPtr = std::unique_ptr<rd_kafka_topic_conf_t, decltype(rd_kafka_topic_conf_destroy) *>;
using KConfCallback = std::function<void(rd_kafka_conf_t *)>;
using KConfParams = std::vector<std::pair<String, String>>;

const char * IDEM_HEADER_NAME = "_idem";

Int32 mapErrorCode(rd_kafka_resp_err_t err)
{
    /// FIXME, more code mapping
    switch (err)
    {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            return ErrorCodes::OK;

        case RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS:
            return ErrorCodes::RESOURCE_ALREADY_EXISTS;

        case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
            /// fallthrough
        case RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:
            /// fallthrough
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
            return ErrorCodes::RESOURCE_NOT_FOUND;

        case RD_KAFKA_RESP_ERR__INVALID_ARG:
            return ErrorCodes::BAD_ARGUMENTS;

        case RD_KAFKA_RESP_ERR__FATAL:
            throw Exception("Fatal error occured, shall tear down the whole program", ErrorCodes::DWAL_FATAL_ERROR);

        default:
            return ErrorCodes::UNKNOWN_EXCEPTION;
    }
}

Int32 doTopic(
    const String & name,
    const std::function<void(rd_kafka_t *, rd_kafka_AdminOptions_t *, rd_kafka_queue_t *)> & do_topic,
    decltype(rd_kafka_event_DeleteTopics_result) topics_result_func,
    decltype(rd_kafka_DeleteTopics_result_topics) topics_func,
    std::function<Int32(const rd_kafka_event_t *)> post_validate,
    rd_kafka_t * handle,
    UInt32 request_timeout,
    Poco::Logger * log,
    const String & action)
{
    /// Setup options
    std::shared_ptr<rd_kafka_AdminOptions_t> options(
        rd_kafka_AdminOptions_new(handle, RD_KAFKA_ADMIN_OP_ANY), rd_kafka_AdminOptions_destroy);

    /// Overall request timeout, including broker lookup, request transmission, operation time on broker and resposne
    /// default is `socket.timeout.ms` which is 60 seconds, -1 for indefinite timeout
    char errstr[512] = {'\0'};
    auto err = rd_kafka_AdminOptions_set_request_timeout(options.get(), request_timeout, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "Failed to {} topic={} error={} detail={}", action, name, rd_kafka_err2str(err), errstr);
        return mapErrorCode(err);
    }

    /// Broker's operation timeout
    err = rd_kafka_AdminOptions_set_operation_timeout(options.get(), request_timeout, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "Failed to {} topic={} error={} detail={}", action, name, rd_kafka_err2str(err), errstr);
        return mapErrorCode(err);
    }

    std::shared_ptr<rd_kafka_queue_t> admin_queue{rd_kafka_queue_new(handle), rd_kafka_queue_destroy};

    /// create or delete topic
    do_topic(handle, options.get(), admin_queue.get());

    /// poll result
    auto rkev = rd_kafka_queue_poll(admin_queue.get(), request_timeout + 500);
    if (rkev == nullptr)
    {
        LOG_ERROR(log, "Failed to {} topic={} timeout", action, name);
        return ErrorCodes::TIMEOUT_EXCEEDED;
    }
    std::shared_ptr<rd_kafka_event_t> event_holder{rkev, rd_kafka_event_destroy};

    if ((err = rd_kafka_event_error(rkev)) != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(
            log,
            "Failed to {} topic={}, error={} detail={}",
            action,
            name,
            rd_kafka_err2str(err),
            rd_kafka_event_error_string(rkev));
        return mapErrorCode(err);
    }

    auto res = topics_result_func(rkev);
    if (res == nullptr)
    {
        LOG_ERROR(log, "Failed to {} topic={}, unknown error", action, name);
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }

    if (topics_func)
    {
        size_t cnt = 0;
        auto result_topics = topics_func(res, &cnt);
        if (cnt != 1 || result_topics == nullptr)
        {
            LOG_ERROR(log, "Failed to {} topic={}, unknown error", action, name);
            return ErrorCodes::UNKNOWN_EXCEPTION;
        }

        if ((err = rd_kafka_topic_result_error(result_topics[0])) != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            LOG_ERROR(
                log,
                "Failed to {} topic={}, error={} detail={}",
                action,
                name,
                rd_kafka_err2str(err),
                rd_kafka_topic_result_error_string(result_topics[0]));

            return mapErrorCode(err);
        }
    }

    if (post_validate)
    {
        return post_validate(res);
    }

    return ErrorCodes::OK;
}

std::unique_ptr<struct rd_kafka_s, void (*)(rd_kafka_t *)>
initRdKafkaHandle(rd_kafka_type_t type, KConfParams & params, DistributedWriteAheadLogKafka::Stats * stats, KConfCallback cb_setup)
{
    KConfPtr kconf{rd_kafka_conf_new(), rd_kafka_conf_destroy};
    if (!kconf)
    {
        LOG_ERROR(stats->log, "Failed to create kafka conf, error={}", rd_kafka_err2str(rd_kafka_last_error()));
        throw Exception("Failed to create kafka conf", mapErrorCode(rd_kafka_last_error()));
    }

    char errstr[512] = {'\0'};
    for (const auto & param : params)
    {
        auto ret = rd_kafka_conf_set(kconf.get(), param.first.c_str(), param.second.c_str(), errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
        {
            LOG_ERROR(stats->log, "Failed to set kafka param_name={} param_value={} error={}", param.first, param.second, ret);
            throw Exception("Failed to create kafka conf", ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

    if (cb_setup)
    {
        cb_setup(kconf.get());
    }

    rd_kafka_conf_set_opaque(kconf.get(), stats);

    std::unique_ptr<struct rd_kafka_s, void (*)(rd_kafka_t *)> kafka_handle(
        rd_kafka_new(type, kconf.release(), errstr, sizeof(errstr)), rd_kafka_destroy);
    if (!kafka_handle)
    {
        LOG_ERROR(stats->log, "Failed to create kafka handle, error={}", errstr);
        throw Exception("Failed to create kafka handle", mapErrorCode(rd_kafka_last_error()));
    }

    return kafka_handle;
}

std::shared_ptr<rd_kafka_topic_t>
initRdKafkaTopicHandle(const String & topic, KConfParams & params, rd_kafka_t * rd_kafka, DistributedWriteAheadLogKafka::Stats * stats)
{
    KTopicConfPtr tconf{rd_kafka_topic_conf_new(), rd_kafka_topic_conf_destroy};
    if (!tconf)
    {
        LOG_ERROR(stats->log, "Failed to create kafka topic conf, error={}", rd_kafka_err2str(rd_kafka_last_error()));
        throw Exception("Failed to created kafka topic conf", mapErrorCode(rd_kafka_last_error()));
    }

    char errstr[512] = {'\0'};
    for (const auto & param : params)
    {
        auto ret = rd_kafka_topic_conf_set(tconf.get(), param.first.c_str(), param.second.c_str(),
                                           errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
        {
            LOG_ERROR(
                stats->log,
                "Failed to set kafka topic param, topic={} param_name={} param_value={} error={}",
                topic,
                param.first,
                param.second,
                errstr);
            throw Exception("Failed to set kafka topic param", ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

    rd_kafka_topic_conf_set_opaque(tconf.get(), stats);

    std::shared_ptr<rd_kafka_topic_t> topic_handle{rd_kafka_topic_new(rd_kafka, topic.c_str(), tconf.release()), rd_kafka_topic_destroy};
    if (!topic_handle)
    {
        LOG_ERROR(
            stats->log, "Failed to create kafka topic handle, topic={} error={}", topic, rd_kafka_err2str(rd_kafka_last_error()));
        throw Exception("Failed to create kafka topic handle", mapErrorCode(rd_kafka_last_error()));
    }

    return topic_handle;
}

inline IDistributedWriteAheadLog::RecordPtr kafkaMsgToRecord(rd_kafka_message_t * msg)
{
    assert(msg != nullptr);

    auto record = IDistributedWriteAheadLog::Record::read(static_cast<const char *>(msg->payload), msg->len);
    if (unlikely(!record))
    {
        return nullptr;
    }

    record->sn = msg->offset;
    record->partition_key = msg->partition;

    rd_kafka_headers_t *hdrs = nullptr;
    if (rd_kafka_message_headers(msg, &hdrs) == RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        /// Has headers
        auto n = rd_kafka_header_cnt(hdrs);
        for (size_t i = 0; i < n; ++i)
        {
            const char * name = nullptr;
            const void * value = nullptr;
            size_t size = 0;

            if (rd_kafka_header_get_all(hdrs, i, &name, &value, &size) == RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                record->headers.emplace(name, String{static_cast<const char*>(value), size});
            }
        }
    }

    return record;
}

int logStats(struct rd_kafka_s * /*rk*/, char * json, size_t json_len, void * opaque)
{
    auto * stats = static_cast<DistributedWriteAheadLogKafka::Stats *>(opaque);
    String stat(json, json + json_len);
    stats->pstat.swap(stat);
    return ErrorCodes::OK;
}

void logErr(struct rd_kafka_s * rk, int err, const char * reason, void * opaque)
{
    auto * stats = static_cast<DistributedWriteAheadLogKafka::Stats *>(opaque);
    if (err == RD_KAFKA_RESP_ERR__FATAL)
    {
        char errstr[512] = {'\0'};
        rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        LOG_ERROR(stats->log, "Fatal error found, error={}", errstr);
    }
    else
    {
        LOG_WARNING(
            stats->log, "Error occured, error={}, reason={}", rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(err)), reason);
    }
}

void logThrottle(struct rd_kafka_s * /*rk*/, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * opaque)
{
    auto * stats = static_cast<DistributedWriteAheadLogKafka::Stats *>(opaque);
    LOG_WARNING(
        stats->log, "Throttling occured on broker={}, broker_id={}, throttle_time_ms={}", broker_name, broker_id, throttle_time_ms);
}

void logOffsetCommits(struct rd_kafka_s * /*rk*/, rd_kafka_resp_err_t err, struct rd_kafka_topic_partition_list_s * offsets, void * opaque)
{
    auto * stats = static_cast<DistributedWriteAheadLogKafka::Stats *>(opaque);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR && err != RD_KAFKA_RESP_ERR__NO_OFFSET)
    {
        LOG_ERROR(stats->log, "Failed to commit offsets, error={}", rd_kafka_err2str(err));
    }

    for (int i = 0; offsets != nullptr && i < offsets->cnt; ++i)
    {
        rd_kafka_topic_partition_t * rktpar = &offsets->elems[i];
        LOG_INFO(
            stats->log,
            "Commits offsets, topic={} partition={} offset={} error={}",
            rktpar->topic,
            rktpar->partition,
            rktpar->offset,
            rd_kafka_err2str(err));
    }
}
}

std::shared_ptr<rd_kafka_topic_s> DistributedWriteAheadLogKafka::initProducerTopic(const DistributedWriteAheadLogKafkaContext & walctx)
{
    assert (inited.test());

    String acks;
    if (settings->enable_idempotence)
    {
        acks = "all";
    }
    else
    {
        acks = std::to_string(walctx.request_required_acks);
    }

    KConfParams topic_params = {
        std::make_pair("request.required.acks", acks),
        /// std::make_pair("delivery.timeout.ms", std::to_string(kLocalMessageTimeout)),
        /// FIXME, partitioner
        std::make_pair("partitioner", "consistent_random"),
        std::make_pair("compression.codec", "inherit"),
    };

    /// rd_kafka_topic_conf_set_partitioner_cb;

    return initRdKafkaTopicHandle(walctx.topic, topic_params, producer_handle.get(), stats.get());
}

std::shared_ptr<rd_kafka_topic_s> DistributedWriteAheadLogKafka::initConsumerTopic(const DistributedWriteAheadLogKafkaContext & walctx)
{
    assert(inited.test());

    KConfParams topic_params = {
        /// enable auto offset commit
        std::make_pair("enable.auto.commit", "true"), /// LEGACY topic settings
        std::make_pair("auto.commit.interval.ms", std::to_string(settings->auto_commit_interval_ms)), /// LEGACY topic settings
        std::make_pair("auto.offset.reset", walctx.auto_offset_reset),
        std::make_pair("consume.callback.max.messages", std::to_string(walctx.consume_callback_max_messages)),
    };

    return initRdKafkaTopicHandle(walctx.topic, topic_params, consumer_handle.get(), stats.get());
}

void DistributedWriteAheadLogKafka::deliveryReport(struct rd_kafka_s *, const rd_kafka_message_s * rkmessage, void * opaque)
{
    if (rkmessage->_private)
    {
        DeliveryReport * report = static_cast<DeliveryReport *>(rkmessage->_private);
        if (rd_kafka_message_status(rkmessage) == RD_KAFKA_MSG_STATUS_PERSISTED)
        {
            /// usually for retried message and idempotent is enabled.
            /// In this case, the message is actually persisted in Kafka broker
            /// the `offset` in delivery report may be -1
            report->err = ErrorCodes::OK;
        }
        else
        {
            report->err = mapErrorCode(rkmessage->err);

            if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                auto * stats = static_cast<DistributedWriteAheadLogKafka::Stats *>(opaque);
                stats->failed += 1;
            }
        }
        report->partition = rkmessage->partition;
        report->offset = rkmessage->offset;

        if (report->callback)
        {
            IDistributedWriteAheadLog::AppendResult result = {
                .sn = rkmessage->offset,
                /// FIXME, mapping to ErrorCodes
                .err = report->err,
                .ctx = rkmessage->partition,
            };
            report->callback(result, report->data);
        }

        if (report->delete_self)
        {
            delete report;
        }
    }
}

DistributedWriteAheadLogKafka::DistributedWriteAheadLogKafka(std::unique_ptr<DistributedWriteAheadLogKafkaSettings> settings_)
    : settings(std::move(settings_))
    , producer_handle(nullptr, rd_kafka_destroy)
    , consumer_handle(nullptr, rd_kafka_destroy)
    , poller(2)
    , log(&Poco::Logger::get("DistributedWriteAheadLogKafka"))
    , stats{std::make_unique<Stats>(log)}
{
}

DistributedWriteAheadLogKafka::~DistributedWriteAheadLogKafka()
{
    shutdown();
}

void DistributedWriteAheadLogKafka::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    initProducer();
    initConsumer();

    poller.scheduleOrThrowOnError([this] { backgroundPollProducer(); });
    poller.scheduleOrThrowOnError([this] { backgroundPollConsumer(); });

    LOG_INFO(log, "Started");
}

void DistributedWriteAheadLogKafka::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");
    poller.wait();
    LOG_INFO(log, "Stopped");
}

void DistributedWriteAheadLogKafka::backgroundPollProducer()
{
    LOG_INFO(log, "Polling producer started");
    setThreadName("KWalPPoller");

    while (!stopped.test())
    {
        rd_kafka_poll(producer_handle.get(), settings->message_delivery_async_poll_ms);
    }

    rd_kafka_resp_err_t ret = rd_kafka_flush(producer_handle.get(), 10000);
    if (ret != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "Failed to flush kafka, error={}", rd_kafka_err2str(ret));
    }
    LOG_INFO(log, "Polling producer stopped");
}

void DistributedWriteAheadLogKafka::backgroundPollConsumer()
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

#if 0
void DistributedWriteAheadLogKafka::flush(std::unordered_map<String, IDistributedWriteAheadLog::RecordPtrs> & buffer)
{
    /// flush the buffer
    for (auto & item : buffer)
    {
        IDistributedWriteAheadLog::ConsumeCallback callback = nullptr;
        void * data = nullptr;
        {
            std::lock_guard lock(consumer_callbacks_mutex);
            auto iter = consumer_callbacks.find(item.first);
            if (iter != consumer_callbacks.end())
            {
                callback = iter->second.first;
                data = iter->second.second;
            }
        }

        if (callback == nullptr)
        {
            LOG_WARNING(log, "There is no callback for consuming message topic$partition={}", item.first);
            item.second.clear();
        }
        else
        {
            callback(std::move(item.second), data);
        }
    }
}

void DistributedWriteAheadLogKafka::backgroundPollConsumer()
{
    auto start = std::chrono::steady_clock::now();
    const Int32 max_buffered = 1000;
    const Int32 max_latency = 100;

    std::unordered_map<String, IDistributedWriteAheadLog::RecordPtrs> buffer;
    Int32 buffered = 0;

    while (!stopped.test())
    {
        rd_kafka_message_t * msg = rd_kafka_consumer_poll(consumer_handle.get(), 100);

        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() >= max_latency
            && buffered > 0)
        {
            flush(buffer);
            start = std::chrono::steady_clock::now();
            buffered = 0;
        }

        if (likely(msg))
        {
            if (msg->err == RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                auto topic = rd_kafka_topic_name(msg->rkt);
                auto key = DistributedWriteAheadLogKafkaContext::topicPartitonKey(topic, msg->partition);
                buffer[key].push_back(from_kafka_msg(msg));
                ++buffered;

                if (buffered >= max_buffered)
                {
                    flush(buffer);
                    start = std::chrono::steady_clock::now();
                    buffered = 0;
                }
            }
            else
            {
                if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
                {
                    LOG_WARNING(
                        log,
                        "Consumer reach end of stream, topic={}, partition={}, offset={}",
                        rd_kafka_topic_name(msg->rkt),
                        msg->partition,
                        msg->offset);
                    /// FIXME, remove the topic partition from subscribe ?
                }

                LOG_WARNING(log, "Consumer failed, error={}", rd_kafka_err2str(msg->err));
            }
        }
    }

    rd_kafka_resp_err_t ret = rd_kafka_consumer_close(consumer_handle.get());
    if (ret != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "Failed to close consumer, error={}", rd_kafka_err2str(ret));
    }
}
#endif

void DistributedWriteAheadLogKafka::initProducer()
{
    std::vector<std::pair<String, String>> producer_params = {
        std::make_pair("bootstrap.servers", settings->brokers.c_str()),
        std::make_pair("queue.buffering.max.messages", std::to_string(settings->queue_buffering_max_messages)),
        std::make_pair("queue.buffering.max.ms", std::to_string(settings->queue_buffering_max_ms)),
        std::make_pair("message.send.max.retries", std::to_string(settings->message_send_max_retries)),
        std::make_pair("retry.backoff.ms", std::to_string(settings->retry_backoff_ms)),
        std::make_pair("enable.idempotence", std::to_string(settings->enable_idempotence)),
        std::make_pair("compression.codec", settings->compression_codec),
        std::make_pair("statistics.interval.ms", std::to_string(settings->statistic_internal_ms)),
    };

    if (!settings->debug.empty())
    {
        producer_params.emplace_back("debug", settings->debug);
    }

    auto cb_setup = [](rd_kafka_conf_t * kconf)
    {
        rd_kafka_conf_set_stats_cb(kconf, &logStats);
        rd_kafka_conf_set_error_cb(kconf, &logErr);
        rd_kafka_conf_set_throttle_cb(kconf, &logThrottle);

        /// delivery report
        rd_kafka_conf_set_dr_msg_cb(kconf, &DistributedWriteAheadLogKafka::deliveryReport);
    };

    producer_handle = initRdKafkaHandle(RD_KAFKA_PRODUCER, producer_params, stats.get(), cb_setup);
}

void DistributedWriteAheadLogKafka::initConsumer()
{
    /// 1) use simple Kafka consumer
    /// 2) manually manage offset commit
    /// 3) offsets are stored in brokers and on application side.
    ///     3.1) In normal cases, application side offsets overrides offsets in borkers
    ///     3.2) In corruption cases (application offsets corruption), use offsets in borkers
    std::vector<std::pair<String, String>> consumer_params = {
        std::make_pair("bootstrap.servers", settings->brokers.c_str()),
        std::make_pair("group.id", settings->group_id),
        /// enable auto offset commit
        std::make_pair("enable.auto.commit", "true"),
        std::make_pair("auto.commit.interval.ms", std::to_string(settings->auto_commit_interval_ms)),
        std::make_pair("enable.auto.offset.store", "false"),
        std::make_pair("offset.store.method", "broker"),
        std::make_pair("enable.partition.eof", "false"),
        std::make_pair("queued.min.messages", std::to_string(settings->queued_min_messages)),
        std::make_pair("queued.max.messages.kbytes", std::to_string(settings->queued_max_messages_kbytes)),
        /// consumer group membership heartbeat timeout
        /// std::make_pair("session.timeout.ms", ""),
    };

    if (!settings->debug.empty())
    {
        consumer_params.emplace_back("debug", settings->debug);
    }

    auto cb_setup = [](rd_kafka_conf_t * kconf) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_conf_set_stats_cb(kconf, &logStats);
        rd_kafka_conf_set_error_cb(kconf, &logErr);
        rd_kafka_conf_set_throttle_cb(kconf, &logThrottle);

        /// offset commits
        rd_kafka_conf_set_offset_commit_cb(kconf, &logOffsetCommits);
    };

    consumer_handle = initRdKafkaHandle(RD_KAFKA_CONSUMER, consumer_params, stats.get(), cb_setup);

    /// Forward all events to consumer queue. there may have in-balance consuming problems
    /// rd_kafka_poll_set_consumer(consumer_handle.get());
}

IDistributedWriteAheadLog::AppendResult DistributedWriteAheadLogKafka::append(const Record & record, std::any & ctx)
{
    assert(ctx.has_value());
    assert(!record.empty());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    std::unique_ptr<DeliveryReport> dr{new DeliveryReport};

    Int32 err = doAppend(record, dr.get(), walctx);
    if (err != static_cast<Int32>(RD_KAFKA_RESP_ERR_NO_ERROR))
    {
        return handleError(err, record, walctx);
    }

    /// Indefinitely wait for the delivery report
    while (true)
    {
        /// instead of busy loop, do a timed poll
        rd_kafka_poll(producer_handle.get(), settings->message_delivery_sync_poll_ms);
        if (dr->offset.load() != -1)
        {
            return {.sn = dr->offset.load(), .ctx = dr->partition.load()};
        }
        else if (dr->err != static_cast<int32_t>(RD_KAFKA_RESP_ERR_NO_ERROR))
        {
            return handleError(dr->err.load(), record, walctx);
        }
    }
    __builtin_unreachable();
}

Int32 DistributedWriteAheadLogKafka::append(
    const Record & record, IDistributedWriteAheadLog::AppendCallback callback, void * data, std::any & ctx)
{
    assert(ctx.has_value());
    assert(!record.empty());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    std::unique_ptr<DeliveryReport> dr(new DeliveryReport{callback, data, true});

    Int32 err = doAppend(record, dr.get(), walctx);
    if (likely(err == static_cast<Int32>(RD_KAFKA_RESP_ERR_NO_ERROR)))
    {
        /// move the ownership to `delivery_report`
        dr.release();
    }
    else
    {
        handleError(err, record, walctx);
    }
    return mapErrorCode(static_cast<rd_kafka_resp_err_t>(err));
}

Int32 DistributedWriteAheadLogKafka::doAppend(const Record & record, DeliveryReport * dr, DistributedWriteAheadLogKafkaContext & walctx)
{
    if (!walctx.topic_handle)
    {
        walctx.topic_handle = initProducerTopic(walctx);
    }

    const char * key_data = nullptr;
    size_t key_size = 0;

    using KHeadPtr = std::unique_ptr<rd_kafka_headers_t, decltype(rd_kafka_headers_destroy) *>;
    KHeadPtr headers{nullptr, rd_kafka_headers_destroy};

    if (!record.headers.empty())
    {
        /// Setup headers
        KHeadPtr header_ptr{rd_kafka_headers_new(record.headers.size()), rd_kafka_headers_destroy};

        for (const auto & h : record.headers)
        {
            rd_kafka_header_add(header_ptr.get(), h.first.data(), h.first.size(), h.second.data(), h.second.size());
            if (h.first == IDEM_HEADER_NAME)
            {
                key_data = h.first.data();
                key_size = h.first.size();
            }
        }

        headers.swap(header_ptr);
    }

    ByteVector data{Record::write(record)};

#ifdef __GNUC__
#pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wold-style-cast"
#endif /// __GNUC__

#ifdef __clang__
#pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wgnu-statement-expression"
#endif /// __clang__

    /// TODO: without block if queue is full and retry with backoff
    /// return failure if retries don't make it through
    int err = rd_kafka_producev(
        producer_handle.get(),
        /// Topic
        RD_KAFKA_V_RKT(walctx.topic_handle.get()),
        /// Use builtin partitioner which is consistent hashing to select partition
        /// RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
        /// Block if internal queue is full
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_FREE | RD_KAFKA_MSG_F_BLOCK),
        /// Message payload and length. Note we didn't copy the data so the ownership
        /// of data will move moved to producev if it succeeds
        RD_KAFKA_V_VALUE(data.data(), data.size()),
        /// For compaction
        RD_KAFKA_V_KEY(key_data, key_size),
        /// Partioner
        RD_KAFKA_V_PARTITION(record.partition_key),
        /// Headers, the memory ownership will be moved to librdkafka
        /// unless producev fails
        RD_KAFKA_V_HEADERS(headers.get()),
        /// Message opaque, carry back the delivery report
        RD_KAFKA_V_OPAQUE(dr),
        RD_KAFKA_V_END);

#ifdef __clang__
#    pragma clang diagnostic pop
#endif /// __clang__

#ifdef __GNUC__
#    pragma GCC diagnostic pop
#endif  /// __GNUC__

    if (!err)
    {
        /// release the ownership as data will be moved to librdkafka
        data.release();
        headers.release();
    }

    return err;
}

IDistributedWriteAheadLog::AppendResult
DistributedWriteAheadLogKafka::handleError(int err, const Record & record, const DistributedWriteAheadLogKafkaContext & ctx)
{
    auto kerr = static_cast<rd_kafka_resp_err_t>(err);
    LOG_ERROR(
        log,
        "Failed to write record to topic={} partition_key={} error={}",
        ctx.topic,
        record.partition_key,
        rd_kafka_err2str(kerr));

    return {.sn = -1, .err = mapErrorCode(kerr), .ctx = -1};
}

void DistributedWriteAheadLogKafka::poll(Int32 timeout_ms, std::any & /*ctx*/)
{
    rd_kafka_poll(producer_handle.get(), timeout_ms);
}

inline Int32 DistributedWriteAheadLogKafka::initConsumerTopicHandleIfNecessary(DistributedWriteAheadLogKafkaContext & walctx)
{
    if (!walctx.topic_handle)
    {
        walctx.topic_handle = initConsumerTopic(walctx);
        /// Always starts from broker stored offset.
        if (rd_kafka_consume_start(walctx.topic_handle.get(), walctx.partition, RD_KAFKA_OFFSET_STORED) == -1)
        {
            LOG_ERROR(
                log,
                "Failed to start consuming topic={} partition={} offset={} error={}",
                walctx.topic,
                walctx.partition,
                walctx.offset,
                rd_kafka_err2str(rd_kafka_last_error()));

            return mapErrorCode(rd_kafka_last_error());
        }
    }

    return ErrorCodes::OK;
}

Int32 DistributedWriteAheadLogKafka::consume(IDistributedWriteAheadLog::ConsumeCallback callback, void * data, std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    auto err = initConsumerTopicHandleIfNecessary(walctx);
    if (err != 0)
    {
        return err;
    }

    struct WrappedData
    {
        IDistributedWriteAheadLog::ConsumeCallback callback;
        void * data;

        RecordPtrs records;
        DistributedWriteAheadLogKafkaContext & ctx;
        Poco::Logger * log;

        Int64 current_size = 0;
        Int64 current_rows = 0;

        WrappedData(
            IDistributedWriteAheadLog::ConsumeCallback callback_,
            void * data_,
            DistributedWriteAheadLogKafkaContext & ctx_,
            Poco::Logger * log_)
            : callback(callback_), data(data_), ctx(ctx_), log(log_)
        {
            records.reserve(1000);
        }
    };

    WrappedData wrapped_data{callback, data, walctx, log};

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
                            getCurrentExceptionMessage(true, true));
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

    if (rd_kafka_consume_callback(walctx.topic_handle.get(), walctx.partition, walctx.consume_callback_timeout_ms, kcallback, &wrapped_data)
        == -1)
    {
        LOG_ERROR(
            log,
            "Failed to consume topic={} partition={} offset={} error={}",
            walctx.topic,
            walctx.partition,
            walctx.offset,
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
                    walctx.topic,
                    walctx.partition,
                    getCurrentExceptionMessage(true, true));
                throw;
            }
        }
    }

    return ErrorCodes::OK;
}

IDistributedWriteAheadLog::ConsumeResult DistributedWriteAheadLogKafka::consume(UInt32 count, Int32 timeout_ms, std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    auto err = initConsumerTopicHandleIfNecessary(walctx);
    if (err != 0)
    {
        return {.err = mapErrorCode(rd_kafka_last_error()), .records = {}};
    }

    std::unique_ptr<rd_kafka_message_t *, decltype(free) *> rkmessages{
        static_cast<rd_kafka_message_t **>(malloc(sizeof(*rkmessages) * count)), free};
    auto res = rd_kafka_consume_batch(walctx.topic_handle.get(), walctx.partition, timeout_ms, rkmessages.get(), count);

    if (res >= 0)
    {
        ConsumeResult result;
        result.records.reserve(res);

        for (ssize_t idx = 0; idx < res; ++idx)
        {
            auto rkmessage = rkmessages.get()[idx];
            if (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                if (rkmessage->offset < walctx.offset)
                {
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
                        log, "Returns nullptr record when consuming topic={} partition={}", walctx.topic, walctx.partition);
                }
            }
            else
            {
                LOG_ERROR(
                    log,
                    "Failed to consume topic={} partition={} error={}",
                    walctx.topic,
                    walctx.partition,
                    rd_kafka_message_errstr(rkmessage));
            }

            rd_kafka_message_destroy(rkmessages.get()[idx]);
        }
        return result;
    }
    else
    {
        LOG_ERROR(
            log,
            "Failed to consuming topic={} partition={} error={}",
            walctx.topic,
            walctx.partition,
            rd_kafka_err2str(rd_kafka_last_error()));

        return {.err = mapErrorCode(rd_kafka_last_error()), .records = {}};
    }
}

Int32 DistributedWriteAheadLogKafka::stopConsume(std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    if (!walctx.topic_handle)
    {
        LOG_ERROR(log, "Didn't start consuming topic={} partition={} yet", walctx.topic, walctx.partition);
        return ErrorCodes::RESOURCE_NOT_INITED;
    }

    if (rd_kafka_consume_stop(walctx.topic_handle.get(), walctx.partition) == -1)
    {
        LOG_ERROR(
            log,
            "Failed to stop consuming topic={} partition={} error={}",
            walctx.topic,
            walctx.partition,
            rd_kafka_err2str(rd_kafka_last_error()));

        return mapErrorCode(rd_kafka_last_error());
    }

    return ErrorCodes::OK;
}

#if 0
bool DistributedWriteAheadLogKafka::consume(IDistributedWriteAheadLog::ConsumeCallback callback, void * data, std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);

    if (callback)
    {
        using TPPtr = std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(rd_kafka_topic_partition_list_destroy) *>;
        TPPtr partitions{rd_kafka_topic_partition_list_new(0), rd_kafka_topic_partition_list_destroy};

        rd_kafka_topic_partition_list_add(partitions.get(), walctx.topic.c_str(), walctx.partition)->offset = walctx.offset;

        rd_kafka_resp_err_t err = rd_kafka_assign(consumer_handle.get(), partitions.get());
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            LOG_ERROR(
                log,
                "Failed to consume message for topic={} partition={} offset={} error={}",
                walctx.topic,
                walctx.partition,
                walctx.offset,
                rd_kafka_err2str(err));

            return false;
        }

        /// register
        const String & key = walctx.key();
        std::lock_guard lock(consumer_callbacks_mutex);

        /// current assignment
        /// rd_kafka_assignment(consumer_handle.get());
        consumer_callbacks.insert({key, {callback, data}});
    }
    else
    {
        /// unregister
    }

    /// poller.scheduleOrThrowOnError([this] { backgroundPollConsumer(); });
    return true;
}
#endif

Int32 DistributedWriteAheadLogKafka::commit(IDistributedWriteAheadLog::RecordSequenceNumber sequence_number, std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    if (!walctx.topic_handle)
    {
        walctx.topic_handle = initConsumerTopic(walctx);
    }

    LOG_INFO(log, "Stores commit offset={} for topic={} partition={}", sequence_number, walctx.topic, walctx.partition);

    auto err = rd_kafka_offset_store(walctx.topic_handle.get(), walctx.partition, sequence_number);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(
            log,
            "Failed to commit offset={} for topic={} partition={} error={}",
            sequence_number,
            walctx.topic,
            walctx.partition,
            rd_kafka_err2str(err));
        return mapErrorCode(err);
    }

    return ErrorCodes::OK;
}

Int32 DistributedWriteAheadLogKafka::create(const String & name, std::any & ctx)
{
    assert(ctx.has_value());

    auto & walctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);

    rd_kafka_NewTopic_t * topics[1] = {nullptr};

    char errstr[512] = {'\0'};
    topics[0] = rd_kafka_NewTopic_new(name.c_str(), walctx.partitions, walctx.replication_factor, errstr, sizeof(errstr));
    if (errstr[0] != '\0')
    {
        LOG_ERROR(log, "Failed to create topic={} error={}", name, errstr);
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }

    KConfParams params = {
        std::make_pair("compression.type", "lz4"),
        std::make_pair("cleanup.policy", walctx.cleanup_policy),
        std::make_pair("retention.ms", std::to_string(walctx.retention_ms)),
    };

    for (const auto & param : params)
    {
        auto err = rd_kafka_NewTopic_set_config(topics[0], param.first.c_str(), param.second.c_str());
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            LOG_ERROR(log, "Failed to set config for topic={} error={}", name, rd_kafka_err2str(err));
            return mapErrorCode(err);
        }
    }

    std::shared_ptr<rd_kafka_NewTopic_t> topics_holder{topics[0], rd_kafka_NewTopic_destroy};

    auto createTopics = [&](rd_kafka_t * handle,
                            rd_kafka_AdminOptions_t * options,
                            rd_kafka_queue_t * admin_queue) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_CreateTopics(handle, topics, 1, options, admin_queue);
    };

    return doTopic(
        name,
        createTopics,
        rd_kafka_event_CreateTopics_result,
        rd_kafka_CreateTopics_result_topics,
        nullptr,
        producer_handle.get(),
        60000,
        log,
        "create");
}

Int32 DistributedWriteAheadLogKafka::remove(const String & name, std::any & ctx)
{
    assert(ctx.has_value());
    (void)ctx;

    rd_kafka_DeleteTopic_t * topics[1] = {nullptr};
    topics[0] = rd_kafka_DeleteTopic_new(name.c_str());
    std::shared_ptr<rd_kafka_DeleteTopic_t> topics_holder{topics[0], rd_kafka_DeleteTopic_destroy};

    auto deleteTopics = [&](rd_kafka_t * handle,
                            rd_kafka_AdminOptions_t * options,
                            rd_kafka_queue_t * admin_queue) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_DeleteTopics(handle, topics, 1, options, admin_queue);
    };

    return doTopic(
        name,
        deleteTopics,
        rd_kafka_event_DeleteTopics_result,
        rd_kafka_DeleteTopics_result_topics,
        nullptr,
        producer_handle.get(),
        60000,
        log,
        "delete");
}

Int32 DistributedWriteAheadLogKafka::describe(const String & name, std::any & ctx)
{
    assert(ctx.has_value());
    (void)ctx;

    rd_kafka_ConfigResource_t * configs[1];
    configs[0] = rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_TOPIC, name.c_str());
    if (configs[0] == nullptr)
    {
        LOG_ERROR(log, "Failed to describe topic, invalid arguments");
        return ErrorCodes::BAD_ARGUMENTS;
    }
    std::shared_ptr<rd_kafka_ConfigResource_t> config_holder{configs[0], rd_kafka_ConfigResource_destroy};

    auto describeTopics = [&](rd_kafka_t * handle,
                              rd_kafka_AdminOptions_t * options,
                              rd_kafka_queue_t * admin_queue) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_DescribeConfigs(handle, configs, 1, options, admin_queue);
    };

    auto validate = [this, &name](const rd_kafka_event_t * event) -> Int32 {
        /// validate result resources
        size_t cnt = 0;
        auto rconfigs = rd_kafka_DescribeConfigs_result_resources(event, &cnt);
        if (cnt != 1 || rconfigs == nullptr)
        {
            LOG_ERROR(log, "Failed to describe topic={}, unknown error", name);
            return ErrorCodes::UNKNOWN_EXCEPTION;
        }

        auto err = rd_kafka_ConfigResource_error(rconfigs[0]);
        if (err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC || err == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)
        {
            return ErrorCodes::RESOURCE_NOT_FOUND;
        }

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            LOG_ERROR(
                log,
                "Failed to describe topic={}, error={} detail={}",
                name,
                rd_kafka_err2str(err),
                rd_kafka_ConfigResource_error_string(rconfigs[0]));

            return mapErrorCode(err);
        }

        rd_kafka_ConfigResource_configs(rconfigs[0], &cnt);
        if (cnt == 0)
        {
            return ErrorCodes::RESOURCE_NOT_FOUND;
        }

        return ErrorCodes::OK;
    };

    return doTopic(
        name, describeTopics, rd_kafka_event_DescribeConfigs_result, nullptr, validate, producer_handle.get(), 4000, log, "describe");
}
}
