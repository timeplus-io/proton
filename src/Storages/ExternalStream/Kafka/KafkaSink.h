#pragma once

#include <atomic>
#include <cstddef>
#include <librdkafka/rdkafka.h>

#include "Core/Block.h"
#include "Formats/FormatFactory.h"
#include "Interpreters/Context_fwd.h"
#include "Processors/Chunk.h"
#include "Processors/Sinks/SinkToStorage.h"
#include "Storages/ExternalStream/Kafka/Kafka.h"
#include "Storages/ExternalStream/Kafka/WriteBufferFromKafka.h"

namespace Poco
{
class Logger;
}

namespace DB
{

using RdKafkaPtr = std::unique_ptr<rd_kafka_t, void (*)(rd_kafka_t *)>;

class KafkaSink final : public SinkToStorage
{
public:
    KafkaSink(const Kafka * kafka, const Block & header, ContextPtr context, const Poco::Logger * logger);
    ~KafkaSink() override;

    std::string getName() const override { return "KafkaSink"; }

    void consume(Chunk chunk) override;
    void onFinish() override;
    void checkpoint(CheckpointContextPtr) override;

private:
    std::function<void(rd_kafka_t *, const rd_kafka_message_t * msg, void *)> onDrMsg();
    std::function<void(rd_kafka_t *, void *, size_t, rd_kafka_resp_err_t, void *, void *)> onDr();

    const Poco::Logger * log;
    RdKafkaPtr producer;
    std::unique_ptr<WriteBufferFromKafka> wb;
    OutputFormatPtr writer;
    std::thread pollingThread;
    bool isFinished;
};
}
