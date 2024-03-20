#pragma once

#include <KafkaLog/KafkaWALContext.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Processors/Streaming/ISource.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Checkpoint/CheckpointRequest.h>

namespace Poco
{
class Logger;
}

struct rd_kafka_message_s;

namespace DB
{
class Kafka;
class StreamingFormatExecutor;

class KafkaSource final : public Streaming::ISource
{
public:
    KafkaSource(
        Kafka * kafka,
        const Block & header,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr query_context_,
        Int32 shard,
        Int64 offset,
        size_t max_block_size,
        Poco::Logger * log_,
        ExternalStreamCounterPtr external_stream_counter_);

    ~KafkaSource() override;

    String getName() const override { return "KafkaSource"; }

    String description() const override { return fmt::format("topic={}, partition={}", consume_ctx.topic, consume_ctx.partition); }

    Chunk generate() override;

    Int64 lastProcessedSN() const override { return ckpt_data.last_sn; }

private:
    void calculateColumnPositions();
    void initConsumer(const Kafka * kafka);
    void initFormatExecutor(const Kafka * kafka);

    static void parseMessage(void * kmessage, size_t total_count, void * data);
    void doParseMessage(const rd_kafka_message_s * kmessage, size_t total_count);
    void parseFormat(const rd_kafka_message_s * kmessage);

    inline void readAndProcess();

    Chunk doCheckpoint(CheckpointContextPtr ckpt_ctx_) override;
    void doRecover(CheckpointContextPtr ckpt_ctx_) override;
    void doResetStartSN(Int64 sn) override;

private:
    StorageSnapshotPtr storage_snapshot;
    ContextPtr query_context;
    size_t max_block_size;
    Poco::Logger * log;

    Block header;
    const Block non_virtual_header;
    Block physical_header;
    Chunk header_chunk;

    std::shared_ptr<ExpressionActions> convert_non_virtual_to_physical_action = nullptr;

    klog::KafkaWALSimpleConsumerPtr consumer;
    klog::KafkaWALContext consume_ctx;

    std::unique_ptr<StreamingFormatExecutor> format_executor;
    ReadBufferFromMemory read_buffer;

    std::vector<std::function<Field(const rd_kafka_message_s *)>> virtual_col_value_functions;
    std::vector<DataTypePtr> virtual_col_types;

    bool request_virtual_columns = false;

    std::optional<String> format_error;
    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;
    MutableColumns current_batch;

    UInt32 record_consume_batch_count = 1000;
    Int32 record_consume_timeout_ms = 100;

    /// For checkpoint
    struct State
    {
        void serialize(WriteBuffer & wb) const;
        void deserialize(VersionType version, ReadBuffer & rb);

        static constexpr VersionType VERSION = 0; /// Current State Version

        /// For VERSION-0
        const String & topic;
        Int32 partition;
        Int64 last_sn = -1;

        explicit State(const klog::KafkaWALContext & consume_ctx_) : topic(consume_ctx_.topic), partition(consume_ctx_.partition) { }
    } ckpt_data;

    ExternalStreamCounterPtr external_stream_counter;
};

}
