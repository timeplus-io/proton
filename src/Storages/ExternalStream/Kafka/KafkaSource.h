#pragma once

#include <Checkpoint/CheckpointRequest.h>
#include <IO/Kafka/Client.h>
#include <IO/ReadBufferFromMemory.h>
#include <Processors/Streaming/ISource.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Stopwatch.h>
#include <Common/TimeBasedThrottler.h>
#include <Storages/ExternalStream/ExternalStreamSource.h>

struct rd_kafka_message_s;

namespace DB
{
class StreamingFormatExecutor;

namespace ExternalStream
{

class Kafka;

class KafkaSource final : public Streaming::ISource, public ExternalStreamSource
{
public:
    KafkaSource(
        const Block & header_,
        const StorageSnapshotPtr & storage_snapshot_,
        const String & data_format,
        const FormatSettings & format_settings,
        const String & topic,
        DB::Kafka::ConsumerPtr consumer_,
        Int32 shard_,
        Int64 offset_,
        std::optional<Int64> high_watermark_,
        size_t max_block_size_,
        UInt64 consumer_stall_timeout_ms,
        ExternalStreamCounterPtr external_stream_counter_,
        Poco::Logger * logger_,
        ContextPtr query_context_);

    ~KafkaSource() override;

    String getName() const override { return "KafkaSource"; }

    String description() const override { return fmt::format("topic={},partition={}", consumer->topicName(), shard); }

    Chunk generate() override;

protected:
    void onCancel() override;

private:
    class StallDetector
    {
    public:
        StallDetector(DB::Kafka::Consumer &, Int32 partition_, Int64 initial_offset, UInt64 timeout_ms_, Poco::Logger *);
        /// Checks if the consumer is stalled, if so, recreate it.
        void checkAndHandleStall();

        Int64 recordedLatestSN() const { return recorded_latest_sn; }

    private:
        void reset();

        /// The consumer is owned by a KafkaSource, which also owns the StallDetector instance.
        /// Thus holding a Consumer refernce is safe here.
        DB::Kafka::Consumer & consumer;
        Int32 partition{-1};
        UInt64 timeout_ms{0};
        Int64 recorded_last_processed_sn{0};
        Int64 recorded_latest_sn{0};
        Stopwatch timer;
        Stopwatch caught_up_timer;
        Poco::Logger * logger;
    };

    /// \brief Parse a Kafka message with the input format.
    void parseFormat(const rd_kafka_message_s * kmessage);

    inline void readAndProcess();

    Chunk doCheckpoint(CheckpointContextPtr ckpt_ctx_) override;
    void doRecover(CheckpointContextPtr ckpt_ctx_) override;
    void doResetStartSN(Int64 sn) override;

    void getPhysicalHeader() override;

    const String topic;

    std::vector<std::function<Field(const rd_kafka_message_s *)>> virtual_col_value_functions;
    std::vector<DataTypePtr> virtual_col_types;

    bool request_virtual_columns = false;

    std::vector<std::pair<Chunk, Int64>> result_chunks_with_sns;
    std::vector<std::pair<Chunk, Int64>>::iterator iter;
    MutableColumns current_batch;

    UInt32 record_consume_batch_count = 1000;
    Int32 record_consume_timeout_ms = 100;

    Int32 shard;
    Int64 offset;
    Int64 high_watermark;
    DB::Kafka::ConsumerPtr consumer;

    /// Indicates that the source has already consumed all messages it is supposed to read [for non-streaming queries].
    bool reached_the_end = false;

    std::unique_ptr<TimeBasedThrottler> watermark_error_log_throttler;

    StallDetector stall_detector;

    ExternalStreamCounterPtr external_stream_counter;

    Poco::Logger * logger;
};

}

}
