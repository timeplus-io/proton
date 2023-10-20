#include "KafkaSink.h"

#include <Processors/Formats/IOutputFormat.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int MISSING_ACKNOWLEDGEMENT;
extern const int INVALID_CONFIG_PARAMETER;
}

KafkaSink::KafkaSink(const Kafka * kafka, const Block & header, ContextPtr context, Poco::Logger * log_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID), producer(nullptr, nullptr), polling_threads(1), log(log_)
{
    /// default values
    std::vector<std::pair<String, String>> producer_params{
        {"enable.idempotence", "true"},
        {"message.timeout.ms", "0" /* infinite */},
    };

    static const std::unordered_set<String> allowed_properties{
        "enable.idempotence",
        "message.timeout.ms",
        "queue.buffering.max.messages",
        "queue.buffering.max.kbytes",
        "queue.buffering.max.ms",
        "message.send.max.retries",
        "retries",
        "retry.backoff.ms",
        "retry.backoff.max.ms",
        "batch.num.messages",
        "batch.size",
        "compression.codec",
        "compression.type",
        "compression.level",
    };

    /// customization, overrides default values
    for (const auto & pair : kafka->properties())
    {
        if (allowed_properties.contains(pair.first))
        {
            producer_params.emplace_back(pair.first, pair.second);
            continue;
        }
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unsupported property {}", pair.first);
    }

    /// properies from settings have higher priority
    producer_params.emplace_back("bootstrap.servers", kafka->brokers());
    producer_params.emplace_back("security.protocol", kafka->securityProtocol());
    if (boost::iequals(kafka->securityProtocol(), "SASL_SSL"))
    {
        producer_params.emplace_back("sasl.mechanisms", "PLAIN");
        producer_params.emplace_back("sasl.username", kafka->username());
        producer_params.emplace_back("sasl.password", kafka->password());
    }

    auto * conf = rd_kafka_conf_new();
    char errstr[512]{'\0'};
    for (const auto & param : producer_params)
    {
        auto ret = rd_kafka_conf_set(conf, param.first.c_str(), param.second.c_str(), errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
        {
            rd_kafka_conf_destroy(conf);
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Failed to set kafka config `{}` with value `{}` error={}",
                param.first,
                param.second,
                ret);
        }
    }

    rd_kafka_conf_set_dr_msg_cb(conf, [](rd_kafka_t * /* producer */, const rd_kafka_message_t * msg, void * /* opaque */) {
        static_cast<WriteBufferFromKafka *>(msg->_private)->onMessageDelivery(msg);
    });

    producer = klog::KafkaPtr(rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)), rd_kafka_destroy);
    if (!producer)
    {
        // librdkafka will take the ownership of `conf` if `rd_kafka_new` succeeds,
        // but if it does not, we need to take care of cleaning it up by ourselves.
        rd_kafka_conf_destroy(conf);
        throw Exception("Failed to create kafka handle", klog::mapErrorCode(rd_kafka_last_error()));
    }

    auto topic = klog::KTopicPtr(rd_kafka_topic_new(producer.get(), kafka->topic().c_str(), nullptr), rd_kafka_topic_destroy);

    wb = std::make_unique<WriteBufferFromKafka>(std::move(topic));

    String data_format = kafka->dataFormat();
    if (data_format.empty())
        data_format = "JSONEachRow";

    writer = FormatFactory::instance().getOutputFormat(data_format, *wb, header, context);
    writer->setAutoFlush();

    polling_threads.scheduleOrThrowOnError([this]() {
        while (!is_finished.test())
            if (auto n = rd_kafka_poll(producer.get(), POLL_TIMEOUT_MS))
                LOG_TRACE(log, "polled {} events", n);
    });
}

void KafkaSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    writer->write(block);
}

void KafkaSink::onFinish()
{
    if (is_finished.test_and_set())
        return;

    polling_threads.wait();

    /// if there are no outstandings, no need to do flushing
    if (wb->hasNoOutstandings())
        return;

    /// Make sure all outstanding requests are transmitted and handled.
    if (auto err = rd_kafka_flush(producer.get(), -1 /* blocks */); err)
        LOG_ERROR(log, "Failed to flush kafka producer, error={}", rd_kafka_err2str(err));

    if (auto err = wb->lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(log, "Failed to send messages, last_seen_error={}", rd_kafka_err2str(err));

    /// if flush does not return an error, the delivery report queue should be empty
    if (!wb->hasNoOutstandings())
        LOG_ERROR(log, "Not all messsages are sent successfully, expected={} actual={}", wb->outstandings(), wb->acked());
}

KafkaSink::~KafkaSink()
{
    onFinish();
}

void KafkaSink::checkpoint(CheckpointContextPtr context)
{
    do
    {
        if (auto err = wb->lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
            throw Exception(
                klog::mapErrorCode(err), "Failed to send messages, error_cout={} last_error={}", wb->error_count(), rd_kafka_err2str(err));

        if (wb->hasNoOutstandings())
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (!is_finished.test());

    wb->resetState();
    IProcessor::checkpoint(context);
}
}
