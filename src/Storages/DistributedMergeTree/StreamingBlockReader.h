#pragma once

#include <DistributedWALClient/KafkaWALContext.h>
#include <DistributedWALClient/KafkaWALSimpleConsumer.h>

namespace Poco
{
    class Logger;
}

namespace DB
{
class IStorage;

/// StreamingBlockReader read blocks from streaming storage
class StreamingBlockReader final : DWAL::SchemaProvider
{
public:
    StreamingBlockReader(
        std::shared_ptr<IStorage> storage_,
        Int32 shard_,
        Int64 offset,
        std::vector<size_t> column_positions,
        const DWAL::KafkaWALSimpleConsumerPtr & consumer_,
        Poco::Logger * log_);

    ~StreamingBlockReader() override;

    const Block & getSchema(UInt16 schema_version) const override;

    DWAL::RecordPtrs read(UInt32 count, Int32 timeout_ms);

private:
    std::shared_ptr<IStorage> storage;
    Block header;

    DWAL::KafkaWALSimpleConsumerPtr consumer;
    DWAL::KafkaWALContext consume_ctx;

    Poco::Logger * log;
};
}
