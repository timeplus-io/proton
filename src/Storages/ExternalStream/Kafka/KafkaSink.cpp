#include <boost/algorithm/string/predicate.hpp>
#include <chrono>
#include <fmt/core.h>
#include <memory>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "Common/Exception.h"
#include "Common/logger_useful.h"
#include "Checkpoint/CheckpointContext.h"
#include "Checkpoint/CheckpointContextFwd.h"
#include "Core/Block.h"
#include "Formats/FormatFactory.h"
#include "Interpreters/Context.h"
#include "KafkaLog/KafkaWALCommon.h"
#include "KafkaSink.h"
#include "Processors/Chunk.h"
#include "Processors/Formats/IOutputFormat.h"
#include "Processors/ProcessorID.h"
#include "Storages/ExternalStream/ExternalStreamTypes.h"
#include "Storages/ExternalStream/Kafka/Kafka.h"
#include "Storages/ExternalStream/Kafka/WriteBufferFromKafka.h"

namespace DB
{

namespace ErrorCodes
{
extern const int MISSING_ACKNOWLEDGEMENT;
extern const int INVALID_CONFIG_PARAMETER;
}

KafkaSink::KafkaSink(const Kafka * kafka, const Block & header, ContextPtr context, const Poco::Logger * logger)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID), log(logger), producer(nullptr, nullptr)
{
    is_streaming = true;

    /// default values
    std::vector<std::pair<std::string, std::string>> producer_params{
        std::make_pair("enable.idempotence", "true"),
        std::make_pair("message.timeout.ms", "0" /* infinite */),
    };

    static const std::unordered_set<std::string> allowed_properties{
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
        throw Exception("Unsupported property: " + pair.first, ErrorCodes::INVALID_CONFIG_PARAMETER);
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

    char errstr[512] = {'\0'};
    for (const auto & param : producer_params)
    {
        auto ret = rd_kafka_conf_set(conf, param.first.c_str(), param.second.c_str(), errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
        {
            rd_kafka_conf_destroy(conf);
            LOG_ERROR(log, "Failed to set kafka config `{}` with value `{}` error={}", param.first, param.second, ret);
            throw Exception("Failed to create kafka config", ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
    }

    rd_kafka_conf_set_dr_msg_cb(conf, [](rd_kafka_t * /* producer */, const rd_kafka_message_t * msg, void * /* opaque */) {
        static_cast<WriteBufferFromKafka *>(msg->_private)->onDrMsg(msg);
    });

    producer = RdKafkaPtr(rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)), rd_kafka_destroy);
    if (!producer)
    {
        // librdkafka will take the ownership of `conf` if `rd_kafka_new` succeeds,
        // but if it does not, we need to take care of cleaning it up by ourselves.
        rd_kafka_conf_destroy(conf);
        throw Exception("Failed to create kafka handle", klog::mapErrorCode(rd_kafka_last_error()));
    }

    auto topic = RdKafkaTopicPtr(rd_kafka_topic_new(producer.get(), kafka->topic().c_str(), nullptr), rd_kafka_topic_destroy);

    wb = std::make_unique<WriteBufferFromKafka>(log, std::move(topic));

    std::string data_format = kafka->dataFormat();
    if (data_format.empty())
        data_format = "JSONEachRow";

    writer = FormatFactory::instance().getOutputFormat(data_format, *wb, header, context);
    writer->setAutoFlush();

    pollingThread = std::thread([this]() {
        while (!isFinished)
        {
            auto n = rd_kafka_poll(producer.get(), 500 /* ms */);
            LOG_TRACE(log, "polled {} events", n);
        }
    });
}

void KafkaSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    auto blk = Block(getHeader());
    blk.setColumns(chunk.getColumns());
    writer->write(blk);
}

void KafkaSink::onFinish()
{
    isFinished = true;
    if (pollingThread.joinable())
        pollingThread.join();

    // Make sure all outstanding requests are transmitted and handled.
    if (auto err = rd_kafka_flush(producer.get(), -1 /* blocks */); err)
        throw Exception(std::string("Failed to flush kafka producer: ") + rd_kafka_err2str(err), klog::mapErrorCode(err));

    if (auto err = wb->deliveryError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw Exception(std::string("Failed to send messages: ") + rd_kafka_err2str(err), klog::mapErrorCode(err));

    // if flush does not return an error, the delivery report queue should be empty
    if (!wb->fullyAcked())
        throw Exception(
            fmt::format("Not all messsages are acked, expected={} actual={}", wb->outstandings(), wb->acked()),
            ErrorCodes::MISSING_ACKNOWLEDGEMENT);
}

KafkaSink::~KafkaSink()
{
    onFinish();
}

void KafkaSink::checkpoint(CheckpointContextPtr context)
{
    do
    {
        if (auto err = wb->deliveryError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
            throw Exception(std::string("Failed to send messages: ") + rd_kafka_err2str(err), klog::mapErrorCode(err));

        if (wb->fullyAcked())
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (true);

    wb->resetState();
    IProcessor::checkpoint(context);
}
}
