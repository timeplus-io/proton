#pragma once


#include <DataStreams/IBlockInputStream.h>
#include <DistributedWriteAheadLog/KafkaWALSimpleConsumer.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
class StreamingBlockReader;
class IStorage;

class StreamingBlockInputStream final : public IBlockInputStream
{
public:
    StreamingBlockInputStream(
        const std::shared_ptr<IStorage> & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Names & column_names_,
        ContextPtr context_,
        Int32 shard_,
        DWAL::KafkaWALSimpleConsumerPtr consumer_,
        Poco::Logger * log_);

    ~StreamingBlockInputStream() override = default;

    String getName() const override { return "StreamingBlockInputStream"; }
    Block getHeader() const override;

private:
    void readPrefixImpl() override;
    Block readImpl() override;
    void readAndProcess();

private:
    std::shared_ptr<IStorage> storage;
    ContextPtr context;
    Names column_names;

    Int32 shard;
    DWAL::KafkaWALSimpleConsumerPtr consumer;
    Poco::Logger * log;

    Block header;
    ColumnWithTypeAndName * wend_type = nullptr;

    std::unique_ptr<StreamingBlockReader> reader;

    Blocks result_blocks;
    Blocks::iterator iter;

    /// watermark, only support periodical flush for now
    // FIXME, late event etc, every second
    Int64 flush_interval_ms = 1000;
    Int64 last_flush_ms = 0;
};
}
