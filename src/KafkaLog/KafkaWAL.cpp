#include "KafkaWAL.h"
#include "KafkaWALCommon.h"
#include "KafkaWALStats.h"

#include <base/logger_useful.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int RESOURCE_NOT_FOUND;
    extern const int RESOURCE_NOT_INITED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_EXCEPTION;
    extern const int BAD_ARGUMENTS;
}
}

namespace klog
{
namespace
{
    int32_t doTopic(
        const std::string & name,
        const std::function<void(rd_kafka_t *, rd_kafka_AdminOptions_t *, rd_kafka_queue_t *)> & do_topic,
        decltype(rd_kafka_event_DeleteTopics_result) topics_result_func,
        decltype(rd_kafka_DeleteTopics_result_topics) topics_func,
        std::function<int32_t(const rd_kafka_event_t *)> post_validate,
        rd_kafka_t * handle,
        uint32_t request_timeout,
        Poco::Logger * log,
        const std::string & action)
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
        auto * rkev = rd_kafka_queue_poll(admin_queue.get(), request_timeout + 500);
        if (rkev == nullptr)
        {
            LOG_ERROR(log, "Failed to {} topic={} timeout", action, name);
            return DB::ErrorCodes::TIMEOUT_EXCEEDED;
        }
        std::shared_ptr<rd_kafka_event_t> event_holder{rkev, rd_kafka_event_destroy};

        if ((err = rd_kafka_event_error(rkev)) != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            LOG_ERROR(
                log, "Failed to {} topic={}, error={} detail={}", action, name, rd_kafka_err2str(err), rd_kafka_event_error_string(rkev));
            return mapErrorCode(err);
        }

        const auto * res = topics_result_func(rkev);
        if (res == nullptr)
        {
            LOG_ERROR(log, "Failed to {} topic={}, unknown error", action, name);
            return DB::ErrorCodes::UNKNOWN_EXCEPTION;
        }

        if (topics_func)
        {
            size_t cnt = 0;
            auto * result_topics = topics_func(res, &cnt);
            if (cnt != 1 || result_topics == nullptr)
            {
                LOG_ERROR(log, "Failed to {} topic={}, unknown error", action, name);
                return DB::ErrorCodes::UNKNOWN_EXCEPTION;
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

        return DB::ErrorCodes::OK;
    }

    std::unique_ptr<rd_kafka_ConfigResource_t, void (*)(rd_kafka_ConfigResource_t *)>
    getTopicConfig(const String & name, rd_kafka_t * handler, Poco::Logger * log)
    {
        rd_kafka_ConfigResource_t * configs[1];
        configs[0] = rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_TOPIC, name.c_str());
        if (configs[0] == nullptr)
        {
            LOG_ERROR(log, "Failed to describe topic, invalid arguments");
            return {configs[0], rd_kafka_ConfigResource_destroy};
        }

        auto describeTopics = [&](rd_kafka_t * handle,
                                  rd_kafka_AdminOptions_t * options,
                                  rd_kafka_queue_t * admin_queue) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
            rd_kafka_DescribeConfigs(handle, configs, 1, options, admin_queue);
        };

        auto get_config = [&name, &configs, &log](const rd_kafka_event_t * event) -> int32_t {
            /// Validate result resources
            size_t cnt = 0;
            auto * rconfigs = rd_kafka_DescribeConfigs_result_resources(event, &cnt);
            if (cnt != 1 || rconfigs == nullptr)
            {
                LOG_ERROR(log, "Failed to describe topic={}, unknown error", name);
                return DB::ErrorCodes::UNKNOWN_EXCEPTION;
            }

            auto err = rd_kafka_ConfigResource_error(rconfigs[0]);
            if (err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC || err == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)
            {
                return DB::ErrorCodes::RESOURCE_NOT_FOUND;
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

            cnt = 0;
            const rd_kafka_ConfigEntry_t ** entries = rd_kafka_ConfigResource_configs(rconfigs[0], &cnt);
            if (cnt == 0)
            {
                return DB::ErrorCodes::RESOURCE_NOT_FOUND;
            }

            /* Apply all existing configuration entries to resource object that
         * will later be passed to AlterConfigs. */
            for (size_t i = 0; i < cnt; i++)
            {
                err = rd_kafka_ConfigResource_set_config(
                    configs[0], rd_kafka_ConfigEntry_name(entries[i]), rd_kafka_ConfigEntry_value(entries[i]));

                if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
                {
                    LOG_ERROR(
                        log,
                        "Failed to set read-back config {}={} on local resource object",
                        rd_kafka_ConfigEntry_name(entries[i]),
                        rd_kafka_ConfigEntry_value(entries[i]));
                    return mapErrorCode(err);
                }
            }

            return DB::ErrorCodes::OK;
        };

        auto err
            = doTopic(name, describeTopics, rd_kafka_event_DescribeConfigs_result, nullptr, get_config, handler, 4000, log, "describe");
        if (err != DB::ErrorCodes::OK)
            return {nullptr, rd_kafka_ConfigResource_destroy};

        return {configs[0], rd_kafka_ConfigResource_destroy};
    }
}

void KafkaWAL::initConsumerTopicHandle(KafkaWALContext & ctx) const
{
    assert(inited.test());

    consumer->initTopicHandle(ctx);
}

void KafkaWAL::initProducerTopicHandle(KafkaWALContext & ctx) const
{
    assert(inited.test());

    std::string acks;
    if (settings->enable_idempotence)
    {
        acks = "all";
    }
    else
    {
        acks = std::to_string(ctx.request_required_acks);
    }

    KConfParams topic_params = {
        std::make_pair("request.required.acks", acks),
        /// std::make_pair("delivery.timeout.ms", std::to_string(kLocalMessageTimeout)),
        /// FIXME, partitioner
        std::make_pair("partitioner", "consistent_random"),
        std::make_pair("compression.codec", ctx.client_side_compression ? "none" : "snappy"),
    };

    /// rd_kafka_topic_conf_set_partitioner_cb;

    ctx.topic_handle = initRdKafkaTopicHandle(ctx.topic, topic_params, producer_handle.get(), stats.get());
}

void KafkaWAL::deliveryReport(struct rd_kafka_s *, const rd_kafka_message_s * rkmessage, void * opaque)
{
    bool failed = false;
    if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR && rd_kafka_message_status(rkmessage) != RD_KAFKA_MSG_STATUS_PERSISTED)
    {
        auto * stats = static_cast<KafkaWALStats *>(opaque);
        stats->failed += 1;
        failed = true;
    }

    if (rkmessage->_private == nullptr)
        return;

    DeliveryReport * report = static_cast<DeliveryReport *>(rkmessage->_private);
    if (!failed)
        /// Usually for retried message and idempotent is enabled.
        /// In this case, the message is actually persisted in Kafka broker
        /// the `offset` in delivery report may be -1
        report->err = DB::ErrorCodes::OK;
    else
        report->err = mapErrorCode(rkmessage->err);

    report->partition = rkmessage->partition;
    report->offset = rkmessage->offset;

    if (report->callback)
    {
        AppendResult result = {
            .err = report->err,
            .sn = rkmessage->offset,
            .partition = rkmessage->partition,
        };

        /// Since deliveryReport is invoked in the poller thread
        /// we will need be extremely careful the `callback` and
        /// `data`'s lifetime are still valid here.
        report->callback(result, report->data);
    }

    if (report->delete_self)
        delete report;
}

KafkaWAL::KafkaWAL(std::unique_ptr<KafkaWALSettings> settings_)
    : settings(std::move(settings_))
    , producer_handle(nullptr, rd_kafka_destroy)
    , consumer(std::make_unique<KafkaWALSimpleConsumer>(settings->clone()))
    , poller(std::make_unique<ThreadPool>(1))
    , log(&Poco::Logger::get("KafkaWAL"))
    , stats{std::make_unique<KafkaWALStats>("producer", log)}
{
}

KafkaWAL::~KafkaWAL()
{
    shutdown();
    LOG_INFO(log, "dtored");
}

void KafkaWAL::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    initProducerHandle();

    poller->scheduleOrThrowOnError([this] { backgroundPollProducer(); });

    consumer->startup();

    LOG_INFO(log, "Started");
}

void KafkaWAL::shutdown()
{
    /// Flush first
    rd_kafka_resp_err_t ret = rd_kafka_flush(producer_handle.get(), 10000);
    if (ret != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(log, "Failed to flush kafka, error={}", rd_kafka_err2str(ret));

    if (stopped.test_and_set())
        return;

    LOG_INFO(log, "Stopping");

    consumer->shutdown();
    poller->wait();
    /// Force thread pool deletion
    poller.reset();

    LOG_INFO(log, "Stopped");
}

void KafkaWAL::backgroundPollProducer() const
{
    LOG_INFO(log, "Polling producer started");
    setThreadName("KWalPPoller");

    /// rd_kafka_poll is polling the delivery report of a message appended
    /// The associated callback will be invoked in this thread
    while (!stopped.test())
        rd_kafka_poll(producer_handle.get(), settings->message_delivery_async_poll_ms);

    rd_kafka_resp_err_t ret = rd_kafka_flush(producer_handle.get(), 10000);
    if (ret != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(log, "Failed to flush kafka, error={}", rd_kafka_err2str(ret));

    LOG_INFO(log, "Polling producer stopped");
}

void KafkaWAL::initProducerHandle()
{
    std::vector<std::pair<std::string, std::string>> producer_params = {
        /// Alias for `metadata.broker.list`
        std::make_pair("bootstrap.servers", settings->brokers.c_str()),
        /// Maximum number of messages allowed on the producer queue.
        std::make_pair("queue.buffering.max.messages", std::to_string(settings->queue_buffering_max_messages)),
        std::make_pair("queue.buffering.max.kbytes", std::to_string(settings->queue_buffering_max_kbytes)),
        std::make_pair("queue.buffering.max.ms", std::to_string(settings->queue_buffering_max_ms)),
        std::make_pair("message.send.max.retries", std::to_string(settings->message_send_max_retries)),
        /// The backoff time in milliseconds before retrying a protocol request.
        std::make_pair("retry.backoff.ms", std::to_string(settings->retry_backoff_ms)),
        /// the producer will ensure that messages are successfully produced exactly once
        std::make_pair("enable.idempotence", boolToString(settings->enable_idempotence)),
        /// librdkafka statistics emit interval time.
        std::make_pair("statistics.interval.ms", std::to_string(settings->statistic_internal_ms)),
        /// Maximum Kafka protocol request message size
        std::make_pair("message.max.bytes", std::to_string(settings->message_max_bytes)),
        std::make_pair("message.timeout.ms", std::to_string(settings->message_timeout_ms)),
        /// Protocol used to communicate with brokers.
        std::make_pair("security.protocol", settings->auth.security_protocol.c_str()),
        std::make_pair("topic.metadata.refresh.interval.ms", std::to_string(settings->topic_metadata_refresh_interval_ms)),
        std::make_pair("compression.codec", settings->compression_codec.c_str()),
    };

    if (!settings->debug.empty())
        producer_params.emplace_back("debug", settings->debug);

    if (boost::iequals(settings->auth.security_protocol, "SASL_SSL"))
    {
        producer_params.emplace_back("sasl.mechanisms", "PLAIN");
        producer_params.emplace_back("sasl.username", settings->auth.username.c_str());
        producer_params.emplace_back("sasl.password", settings->auth.password.c_str());
    }

    auto cb_setup = [](rd_kafka_conf_t * kconf) {
        rd_kafka_conf_set_stats_cb(kconf, &KafkaWALStats::logStats);
        rd_kafka_conf_set_error_cb(kconf, &KafkaWALStats::logErr);
        rd_kafka_conf_set_throttle_cb(kconf, &KafkaWALStats::logThrottle);

        /// Delivery report for async append
        rd_kafka_conf_set_dr_msg_cb(kconf, &KafkaWAL::deliveryReport);
    };

    producer_handle = initRdKafkaHandle(RD_KAFKA_PRODUCER, producer_params, stats.get(), cb_setup);
}

AppendResult KafkaWAL::append(nlog::Record & record, const KafkaWALContext & ctx) const
{
    assert(ctx.topic_handle);

    std::unique_ptr<DeliveryReport> dr{new DeliveryReport};

    int32_t err = doAppend(record, dr.get(), ctx);
    if (err != static_cast<int32_t>(RD_KAFKA_RESP_ERR_NO_ERROR))
        return handleError(err, record, ctx);

    /// Indefinitely wait for the delivery report
    while (true)
    {
        /// instead of busy loop, do a timed poll
        rd_kafka_poll(producer_handle.get(), settings->message_delivery_sync_poll_ms);
        if (dr->err != static_cast<int32_t>(RD_KAFKA_RESP_ERR_NO_ERROR))
            return handleError(dr->err.load(), record, ctx);
        else if (dr->offset.load() != -1)
            return {.sn = dr->offset.load(), .partition = dr->partition.load()};
    }
    __builtin_unreachable();
}

int32_t KafkaWAL::append(nlog::Record & record, AppendCallback callback, void * data, const KafkaWALContext & ctx) const
{
    assert(!record.empty());
    assert(ctx.topic_handle);

    std::unique_ptr<DeliveryReport> dr;
    if (callback)
        dr.reset(new DeliveryReport{callback, data, true});

    int32_t err = doAppend(record, dr.get(), ctx);
    if (likely(err == static_cast<int32_t>(RD_KAFKA_RESP_ERR_NO_ERROR)))
        /// Move the ownership to `delivery_report`
        dr.release();
    else
        handleError(err, record, ctx);

    return mapErrorCode(static_cast<rd_kafka_resp_err_t>(err));
}

int32_t KafkaWAL::doAppend(nlog::Record & record, DeliveryReport * dr, const KafkaWALContext & ctx) const
{
    nlog::ByteVector data{record.serialize()};

#ifdef __GNUC__
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wold-style-cast"
#endif /// __GNUC__

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wgnu-statement-expression"
#    pragma clang diagnostic ignored "-Wreserved-identifier"
#endif /// __clang__

    const auto & key = record.getKey();

    /// TODO: without block if queue is full and retry with backoff
    /// return failure if retries don't make it through
    int err = rd_kafka_producev(
        producer_handle.get(),
        /// Topic
        RD_KAFKA_V_RKT(ctx.topic_handle.get()),
        /// Use builtin partitioner which is consistent hashing to select partition
        /// RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
        /// Return RD_KAFKA_RESP_ERR__QUEUE_FULL if internal queue is full
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_FREE | RD_KAFKA_MSG_F_BLOCK),
        /// Message payload and length. Note we didn't copy the data so the ownership
        /// of data will move moved to producev if it succeeds
        RD_KAFKA_V_VALUE(data.data(), data.size()),
        /// For compaction
        RD_KAFKA_V_KEY(key.c_str(), key.size()),
        /// Partitioner
        RD_KAFKA_V_PARTITION(record.getShard()),
        /// Headers, the memory ownership will be moved to librdkafka
        /// unless producev fails
        /// RD_KAFKA_V_HEADERS(headers.get()),
        /// Message opaque, carry back the delivery report
        RD_KAFKA_V_OPAQUE(dr),
        RD_KAFKA_V_END);

#ifdef __clang__
#    pragma clang diagnostic pop
#endif /// __clang__

#ifdef __GNUC__
#    pragma GCC diagnostic pop
#endif /// __GNUC__

    if (!err)
        /// release the ownership as data will be moved to librdkafka
        data.release();

    return err;
}

AppendResult KafkaWAL::handleError(int err, const nlog::Record & record, const KafkaWALContext & ctx) const
{
    auto kerr = static_cast<rd_kafka_resp_err_t>(err);
    LOG_ERROR(log, "Failed to write record to topic={} shard={} error={}", ctx.topic, record.getShard(), rd_kafka_err2str(kerr));

    return {.err = mapErrorCode(kerr)};
}

void KafkaWAL::poll(int32_t timeout_ms, const KafkaWALContext &) const
{
    rd_kafka_poll(producer_handle.get(), timeout_ms);
}

int32_t KafkaWAL::consume(ConsumeCallback callback, ConsumeCallbackData * data, const KafkaWALContext & ctx) const
{
    return consumer->consume(callback, data, ctx);
}

ConsumeResult KafkaWAL::consume(uint32_t count, int32_t timeout_ms, const KafkaWALContext & ctx) const
{
    return consumer->consume(count, timeout_ms, ctx);
}

int32_t KafkaWAL::stopConsume(const KafkaWALContext & ctx) const
{
    return consumer->stopConsume(ctx);
}

int32_t KafkaWAL::commit(nlog::RecordSN sn, const KafkaWALContext & ctx) const
{
    return consumer->commit(sn, ctx);
}

int32_t KafkaWAL::create(const std::string & name, const KafkaWALContext & ctx) const
{
    rd_kafka_NewTopic_t * topics[1] = {nullptr};

    char errstr[512] = {'\0'};
    topics[0] = rd_kafka_NewTopic_new(name.c_str(), ctx.partitions, ctx.replication_factor, errstr, sizeof(errstr));
    if (errstr[0] != '\0')
    {
        LOG_ERROR(log, "Failed to create topic={} error={}", name, errstr);
        return DB::ErrorCodes::UNKNOWN_EXCEPTION;
    }

    KConfParams params = {
        std::make_pair("compression.type", ctx.client_side_compression ? "uncompressed" : "snappy"),
        std::make_pair("cleanup.policy", ctx.cleanup_policy),
    };

    if (ctx.retention_ms > 0)
    {
        params.emplace_back("retention.ms", std::to_string(ctx.retention_ms));
    }

    if (ctx.segment_bytes > 0)
    {
        params.emplace_back("segment.bytes", std::to_string(ctx.segment_bytes));
    }

    if (ctx.segment_ms > 0)
    {
        params.emplace_back("segment.ms", std::to_string(ctx.segment_ms));
    }

    if (ctx.message_max_bytes > 0)
    {
        params.emplace_back("max.message.bytes", std::to_string(ctx.message_max_bytes));
    }
    else
    {
        params.emplace_back("max.message.bytes", std::to_string(settings->message_max_bytes));
    }

    if (ctx.flush_messages > 0)
    {
        params.emplace_back("flush.messages", std::to_string(ctx.flush_messages));
    }

    if (ctx.flush_ms > 0)
    {
        params.emplace_back("flush.ms", std::to_string(ctx.flush_ms));
    }

    if (ctx.use_append_timestamp)
    {
        params.emplace_back("message.timestamp.type", "LogAppendTime");
    }
    else
    {
        params.emplace_back("message.timestamp.type", "CreateTime");
    }

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

    auto create_topics = [&](rd_kafka_t * handle,
                            rd_kafka_AdminOptions_t * options,
                            rd_kafka_queue_t * admin_queue) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_CreateTopics(handle, topics, 1, options, admin_queue);
    };

    return doTopic(
        name,
        create_topics,
        rd_kafka_event_CreateTopics_result,
        rd_kafka_CreateTopics_result_topics,
        nullptr,
        producer_handle.get(),
        60000,
        log,
        "create");
}

int32_t KafkaWAL::alter(const String & name, const std::vector<std::pair<String, String>> & params) const
{
    /// prepare config
    auto config = getTopicConfig(name, producer_handle.get(), log);

    if (!config)
        return DB::ErrorCodes::RESOURCE_NOT_FOUND;

    for (const auto & param : params)
    {
        auto err = rd_kafka_ConfigResource_set_config(config.get(), param.first.c_str(), param.second.c_str());
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            LOG_ERROR(log, "Failed to set config for topic={} error={}", name, rd_kafka_err2str(err));
            return mapErrorCode(err);
        }
    }

    rd_kafka_ConfigResource_t * pconfig = config.get();
    auto alter_topic = [&](rd_kafka_t * handle,
                           rd_kafka_AdminOptions_t * options,
                           rd_kafka_queue_t * admin_queue) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_AlterConfigs(handle, &pconfig, 1, options, admin_queue);
    };

    auto validate = [this, &name](const rd_kafka_event_t * event) -> int32_t {
        /// Validate result resources
        size_t cnt = 0;
        auto * cres = rd_kafka_AlterConfigs_result_resources(event, &cnt);
        assert(cnt == 1);

        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        for (size_t i = 0; i < cnt; i++)
        {
            if (rd_kafka_ConfigResource_error(cres[i]))
            {
                LOG_ERROR(
                    log,
                    "Failed to alter topic={}, ConfigResource result: {}, {}: error: {}",
                    name,
                    rd_kafka_ConfigResource_type(cres[i]),
                    rd_kafka_ConfigResource_name(cres[i]),
                    rd_kafka_ConfigResource_error_string(cres[i]));
                err = rd_kafka_ConfigResource_error(cres[i]);
            }
        }
        return mapErrorCode(err);
    };

    return doTopic(name, alter_topic, rd_kafka_event_AlterConfigs_result, nullptr, validate, producer_handle.get(), 60000, log, "alter");
}

std::map<String, String> KafkaWAL::get(const std::string & name)
{
    std::map<String, String> params;

    auto config = getTopicConfig(name, producer_handle.get(), log);
    if (!config)
        return params;

    size_t cnt = 0;
    const rd_kafka_ConfigEntry_t ** entries = rd_kafka_ConfigResource_configs(config.get(), &cnt);
    if (cnt == 0)
        return params;

    for (size_t i = 0; i < cnt; i++)
        params.emplace(rd_kafka_ConfigEntry_name(entries[i]), rd_kafka_ConfigEntry_value(entries[i]));

    return params;
}

int32_t KafkaWAL::remove(const String & name, const KafkaWALContext &) const
{
    rd_kafka_DeleteTopic_t * topics[1] = {nullptr};
    topics[0] = rd_kafka_DeleteTopic_new(name.c_str());
    std::shared_ptr<rd_kafka_DeleteTopic_t> topics_holder{topics[0], rd_kafka_DeleteTopic_destroy};

    auto delete_topics = [&](rd_kafka_t * handle,
                            rd_kafka_AdminOptions_t * options,
                            rd_kafka_queue_t * admin_queue) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        rd_kafka_DeleteTopics(handle, topics, 1, options, admin_queue);
    };

    return doTopic(
        name,
        delete_topics,
        rd_kafka_event_DeleteTopics_result,
        rd_kafka_DeleteTopics_result_topics,
        nullptr,
        producer_handle.get(),
        60000,
        log,
        "delete");
}

DescribeResult KafkaWAL::describe(const String & name) const
{
    return describeTopic(name, producer_handle.get(), log);
}

KafkaWALClusterPtr KafkaWAL::cluster(const KafkaWALContext & ctx) const
{
    const struct rd_kafka_metadata * metadata = nullptr;

    auto err = rd_kafka_metadata(producer_handle.get(), 0, ctx.topic_handle.get(), &metadata, 5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_ERROR(log, "Failed to get cluster metadata error={}", rd_kafka_err2str(err));
        return nullptr;
    }

    KafkaWALClusterPtr result = std::make_shared<KafkaWALCluster>();
    result->id = settings->cluster_id;
    result->controller_id = rd_kafka_controllerid(producer_handle.get(), 0);

    /// Brokers
    for (int32_t i = 0; i < metadata->broker_cnt; ++i)
    {
        result->nodes.push_back({});
        auto & node = result->nodes.back();
        node.id = metadata->brokers[i].id;
        node.port = metadata->brokers[i].port;
        node.host = metadata->brokers[i].host;
    }

#if 0
    /// Topics
    for (int32_t i = 0; i < metadata->topic_cnt; ++i)
    {
        result->wals.push_back({});
        auto & wal = result->wals.back();
        wal.name = metadata->topics[i].topic;

        /// Partitions
        for (int32_t j = 0; j < metadata->topics[i].partition_cnt; ++j)
        {
            const auto & meta_partition = metadata->topics[i].partitions[j];

            wal.partitions.push_back({});
            auto & partition = wal.partitions.back();
            partition.id = meta_partition.id;
            partition.leader = meta_partition.leader;

            for (int32_t k = 0; k < meta_partition.replica_cnt; ++k)
            {
                partition.replica_nodes.push_back(meta_partition.replicas[k]);
            }

            for (int32_t k = 0; k < meta_partition.isr_cnt; ++k)
            {
                partition.isrs.push_back(meta_partition.isrs[k]);
            }
        }
    }
#endif

    rd_kafka_metadata_destroy(metadata);

    return result;
}

/// https://cwiki.apache.org/confluence/display/KAFKA/KIP-33+-+Add+a+time+based+log+index
std::vector<int64_t> KafkaWAL::offsetsForTimestamps(const std::string & topic, const std::vector<int64_t> & timestamps, int32_t timeout_ms) const
{
    return getOffsetsForTimestamps(producer_handle.get(), topic, timestamps, timeout_ms);
}
}
