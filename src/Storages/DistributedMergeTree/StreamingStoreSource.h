#pragma once

#include "StreamingStoreSourceBase.h"

#include <DistributedWALClient/KafkaWALSimpleConsumer.h>

namespace DB
{
class StreamingBlockReader;
class IStorage;

class StreamingStoreSource final : public StreamingStoreSourceBase
{
public:
    StreamingStoreSource(
        std::shared_ptr<IStorage> storage_,
        const Block & header,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr context_,
        Int32 shard_,
        Int64 offset,
        DWAL::KafkaWALSimpleConsumerPtr consumer_,
        Poco::Logger * log_);

    ~StreamingStoreSource() override = default;

    String getName() const override { return "StreamingStoreSource"; }

private:
    void readAndProcess() override;

private:
    std::shared_ptr<IStorage> storage;

    Int32 shard;
    DWAL::KafkaWALSimpleConsumerPtr consumer;
    Poco::Logger * log;

    std::unique_ptr<StreamingBlockReader> reader;

    UInt64 record_consume_batch_count = 1000;
    Int64 record_consume_timeout = 100;
};

using StreamingStoreSourcePtr = std::shared_ptr<StreamingStoreSource>;
}
