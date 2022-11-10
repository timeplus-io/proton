#pragma once

#include <KafkaLog/KafkaWALContext.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>

namespace Poco
{
    class Logger;
}

namespace DB
{
class StreamShard;

/// StreamingBlockReader read blocks from streaming storage
class StreamingBlockReaderKafka final : nlog::SchemaProvider
{
public:
    StreamingBlockReaderKafka(
        std::shared_ptr<StreamShard> stream_shard_,
        Int64 offset,
        SourceColumnsDescription::PhysicalColumnPositions column_positions,
        klog::KafkaWALSimpleConsumerPtr consumer_,
        Poco::Logger * log_);

    ~StreamingBlockReaderKafka() override;

    const Block & getSchema(UInt16 schema_version) const override;

    nlog::RecordPtrs read(UInt32 count, Int32 timeout_ms);

    std::pair<String, Int32> getStreamShard() const;

    /// Call this function only before read()
    void resetOffset(Int64 offset);

private:
    std::shared_ptr<StreamShard> stream_shard;
    Block schema;

    klog::KafkaWALSimpleConsumerPtr consumer;
    klog::KafkaWALContext consume_ctx;

    Poco::Logger * log;
};
}
