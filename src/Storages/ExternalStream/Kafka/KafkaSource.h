#pragma once

#include <Checkpoint/CheckpointRequest.h>
#include <IO/ReadBufferFromMemory.h>
#include <Processors/Streaming/ISource.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/Kafka/ConsumerPool.h>
#include <Storages/ExternalStream/Kafka/Topic.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>

struct rd_kafka_message_s;

namespace DB
{
class Kafka;
class StreamingFormatExecutor;

class KafkaSource final : public Streaming::ISource
{
public:
    KafkaSource(
        Kafka & kafka_,
        const Block & header_,
        const StorageSnapshotPtr & storage_snapshot_,
        const RdKafka::ConsumerPool::Entry & consumer_,
        RdKafka::TopicPtr topic_,
        Int32 shard_,
        Int64 offset_,
        size_t max_block_size_,
        ExternalStreamCounterPtr external_stream_counter_,
        ContextPtr query_context_);

    ~KafkaSource() override;

    String getName() const override { return "KafkaSource"; }

    String description() const override { return fmt::format("topic={}, partition={}", topic->name(), shard); }

    Chunk generate() override;

    Int64 lastProcessedSN() const override { return ckpt_data.last_sn; }

private:
    void calculateColumnPositions();
    void initFormatExecutor();

    /// \brief Parse a Kafka message and return the offset of current message. (return nullopt if it is not a valid message)
    std::optional<Int64> parseMessage(void * kmessage, size_t total_count, void * data);
    void parseFormat(const rd_kafka_message_s * kmessage);

    inline void readAndProcess();

    Chunk doCheckpoint(CheckpointContextPtr ckpt_ctx_) override;
    void doRecover(CheckpointContextPtr ckpt_ctx_) override;
    void doResetStartSN(Int64 sn) override;

    Kafka & kafka;
    StorageSnapshotPtr storage_snapshot;
    size_t max_block_size;

    Block header;
    const Block non_virtual_header;
    Block physical_header;
    Chunk header_chunk;

    std::shared_ptr<ExpressionActions> convert_non_virtual_to_physical_action = nullptr;

    std::unique_ptr<StreamingFormatExecutor> format_executor;
    ReadBufferFromMemory read_buffer;

    std::vector<std::function<Field(const rd_kafka_message_s *)>> virtual_col_value_functions;
    std::vector<DataTypePtr> virtual_col_types;

    bool request_virtual_columns = false;

    std::optional<String> format_error;
    std::vector<std::pair<Chunk, Int64>> result_chunks_with_sns;
    std::vector<std::pair<Chunk, Int64>>::iterator iter;
    MutableColumns current_batch;

    UInt32 record_consume_batch_count = 1000;
    Int32 record_consume_timeout_ms = 100;

    RdKafka::ConsumerPool::Entry consumer;
    RdKafka::TopicPtr topic;
    Int32 shard;
    Int64 offset;

    bool consume_started = false;

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

        State(const String & topic_, Int32 partition_) : topic(topic_), partition(partition_) { }
    } ckpt_data;

    ExternalStreamCounterPtr external_stream_counter;

    ContextPtr query_context;
    Poco::Logger * logger;
};

}
