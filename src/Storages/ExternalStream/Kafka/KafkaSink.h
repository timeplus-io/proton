#pragma once

#include <Formats/FormatFactory.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Kafka/WriteBufferFromKafka.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class KafkaSink final : public SinkToStorage
{
public:
    KafkaSink(const Kafka * kafka, const Block & header, ContextPtr context, Poco::Logger * logger);
    ~KafkaSink() override;

    String getName() const override { return "KafkaSink"; }

    void consume(Chunk chunk) override;
    void onFinish() override;
    void checkpoint(CheckpointContextPtr) override;

private:
    static const int POLL_TIMEOUT_MS = 500;

    klog::KafkaPtr producer;
    std::unique_ptr<WriteBufferFromKafka> wb;
    OutputFormatPtr writer;
    std::thread pollingThread;
    std::atomic_flag is_finished;

    Poco::Logger * log;
};
}
