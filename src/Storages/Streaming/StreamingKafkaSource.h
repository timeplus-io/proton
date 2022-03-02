#pragma once

#include <Processors/Sources/SourceWithProgress.h>

#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>
#include <Storages/Kafka/StorageKafka.h>

namespace Poco
{
class Logger;
}
namespace DB
{

class StreamingKafkaSource : public SourceWithProgress
{
public:
    StreamingKafkaSource(
        StorageKafka & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        Poco::Logger * log_,
        size_t max_block_size_);
    ~StreamingKafkaSource() override;

    String getName() const override { return "StreamingKafkaSource"; }

    Chunk generate() override;

private:
    void readAndProcess();
    void commit();
    bool isStalled() const { return !buffer || buffer->isStalled(); }

private:
    StorageKafka & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;
    Poco::Logger * log;
    UInt64 max_block_size;

    ConsumerBufferPtr buffer;

    Block header;
    const Block non_virtual_header;
    const Block virtual_header;
    const HandleKafkaErrorMode handle_error_mode;

    std::vector<std::pair<Int32, std::function<Int64(const BlockInfo &)>>> virtual_time_columns_calc;

    /// These virtual columns have the same Int64 type
    DataTypePtr virtual_col_type;

    Chunk header_chunk;

    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;

    /// watermark, only support periodical flush for now
    // FIXME, late event etc, every second
    Int64 flush_interval_ms = 1000;
    Int64 last_flush_ms = 0;
};

}
