#pragma once

#include <KafkaLog/KafkaWALContext.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace Poco
{
class Logger;
}

struct rd_kafka_message_s;

namespace DB
{
class Kafka;
class StreamingFormatExecutor;

class KafkaSource : public SourceWithProgress
{
public:
    KafkaSource(
        Kafka * kafka,
        const Block & header,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr query_context_,
        Int32 shard,
        Int64 offset,
        size_t max_block_size,
        Poco::Logger * log_);

    ~KafkaSource() override;

    String getName() const override { return "KafkaSource"; }

    Chunk generate() override;

private:
    void calculateColumnPositions();
    void initConsumer(const Kafka * kafka);
    void initFormatExecutor(const Kafka * kafka);

    static void parseMessage(void * kmessage, size_t total_count, void * data);
    void doParseMessage(const rd_kafka_message_s * kmessage, size_t total_count);
    void parseFormat(const rd_kafka_message_s * kmessage);
    void parseRaw(const rd_kafka_message_s * kmessage);

    inline void readAndProcess();

private:
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;
    size_t max_block_size;
    Poco::Logger * log;

    Block header;
    Block physical_header;
    Chunk header_chunk;

    klog::KafkaWALSimpleConsumerPtr consumer;
    klog::KafkaWALContext consume_ctx;

    std::unique_ptr<StreamingFormatExecutor> format_executor;
    ReadBufferFromMemory read_buffer;

    std::vector<std::function<Int64(const rd_kafka_message_s *)>> virtual_time_columns_calc;

    /// These virtual columns have the same Int64 type
    DataTypePtr virtual_col_type;

    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;
    MutableColumns current_batch;

    /// watermark, only support periodical flush for now
    /// FIXME, late event etc, every second
    Int64 flush_interval_ms = 1000;
    Int64 last_flush_ms = 0;

    UInt64 record_consume_batch_count = 1000;
    Int64 record_consume_timeout = 100;
};

}
