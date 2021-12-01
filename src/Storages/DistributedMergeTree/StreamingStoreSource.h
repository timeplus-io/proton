#pragma once


#include <DistributedWriteAheadLog/KafkaWALSimpleConsumer.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
class StreamingBlockReader;
class IStorage;

class StreamingStoreSource : public SourceWithProgress
{
public:
    StreamingStoreSource(
        std::shared_ptr<IStorage> storage_,
        const Block & header,
        ContextPtr context_,
        Int32 shard_,
        Int64 offset,
        DWAL::KafkaWALSimpleConsumerPtr consumer_,
        Poco::Logger * log_);

    ~StreamingStoreSource() override = default;

    String getName() const override { return "StreamingStoreSource"; }
    Chunk generate() override;

private:
    void readAndProcess();

private:
    std::shared_ptr<IStorage> storage;
    ContextPtr context;
    Names column_names;

    Chunk header_chunk;

    Int32 shard;
    DWAL::KafkaWALSimpleConsumerPtr consumer;
    Poco::Logger * log;

    std::unique_ptr<StreamingBlockReader> reader;

    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;

    /// watermark, only support periodical flush for now
    // FIXME, late event etc, every second
    Int64 flush_interval_ms = 1000;
    Int64 last_flush_ms = 0;
};
}
