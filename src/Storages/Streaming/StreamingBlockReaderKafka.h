#pragma once

#include <KafkaLog/KafkaWALContext.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>

namespace Poco
{
    class Logger;
}

namespace DB
{
class IStorage;

/// StreamingBlockReader read blocks from streaming storage
class StreamingBlockReaderKafka final : nlog::SchemaProvider
{
public:
    StreamingBlockReaderKafka(
        std::shared_ptr<IStorage> storage_,
        Int32 shard_,
        Int64 offset,
        SourceColumnsDescription::PhysicalColumnPositions column_positions,
        klog::KafkaWALSimpleConsumerPtr consumer_,
        Poco::Logger * log_);

    ~StreamingBlockReaderKafka() override;

    const Block & getSchema(UInt16 schema_version) const override;

    nlog::RecordPtrs read(UInt32 count, Int32 timeout_ms);

private:
    std::shared_ptr<IStorage> storage;
    Block header;

    klog::KafkaWALSimpleConsumerPtr consumer;
    klog::KafkaWALContext consume_ctx;

    Poco::Logger * log;
};
}
