#pragma once

#include <memory>
#include <Checkpoint/CheckpointRequest.h>
#include <Columns/IColumn.h>
#include <IO/Kafka/Client.h>
#include <IO/ReadBufferFromMemory.h>
#include <Processors/Streaming/ISource.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/ExternalStreamSource.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/TimeBasedThrottler.h>
#include <Formats/KafkaSchemaRegistryForAvro.h>


namespace DB
{
class StreamingFormatExecutor;

namespace ExternalStream
{

class Kafka;

/// Processing unit status for parallel parsing
enum class ParseUnitStatus
{
    ReadyToFill,    ///< Ready for reader thread to fill with messages
    ReadyToParse,   ///< Ready for parser thread to process
    ReadyToOutput,  ///< Parsing complete, ready for generate()
};

/// Copied Kafka message for parallel parsing (owned by ParseUnit)
struct CopiedKafkaMessage
{
    std::string payload;
    std::string key;
    int64_t offset;
};

/// Processing unit for parallel parsing
struct alignas(64) ParseUnit
{
    /// Output
    Chunk chunk;
    Streaming::SequenceRange sn_range;
    int64_t last_processed_record_timestamp;
    uint64_t read_bytes;
    std::exception_ptr exception;

    /// Thread-local format executor (created once, reused)
    std::shared_ptr<StreamingFormatExecutor> format_executor;
    std::shared_ptr<StreamingFormatExecutor> format_batch_executor;

    std::string batch_buffer;

    /// Synchronization (each unit has its own mutex/condvar to avoid contention)
    std::mutex mutex;
    std::condition_variable cv;
    ParseUnitStatus status{ParseUnitStatus::ReadyToFill};
};

class KafkaSource final : public Streaming::ISource, public ExternalStreamSource
{
public:
    struct Timeouts
    {
        UInt64 connection_timeout_ms;
        UInt64 consumer_stall_timeout_ms;
    };

    KafkaSource(
        const Block & header_,
        const StorageSnapshotPtr & storage_snapshot_,
        String data_format_,
        const FormatSettings & format_settings,
        String topic_,
        DB::Kafka::ConsumerPtr consumer_,
        Int32 shard_,
        Int64 offset_,
        std::optional<Int64> high_watermark_,
        size_t max_block_size_,
        const Timeouts & timeouts,
        std::shared_ptr<KafkaSchemaRegistryForAvro> avro_key_schema_registry_,
        ExternalStreamCounterPtr external_stream_counter_,
        ContextPtr query_context_,
        LoggerPtr logger_);

    ~KafkaSource() override;

    String getName() const override { return "KafkaSource"; }

    std::pair<Int64, Int64> sequenceRange() const override;

    Chunk generate() override;

protected:
    void onCancel() noexcept override;

private:
    class StallDetector
    {
    public:
        StallDetector(DB::Kafka::Consumer &, Int32 partition_, Int64 initial_offset, const KafkaSource::Timeouts & timeouts_, LoggerPtr);
        /// Checks if the consumer is stalled, if so, recreate it.
        void checkAndHandleStall();

        Int64 recordedLatestSN() const { return recorded_latest_sn; }

    private:
        /// The consumer is owned by a KafkaSource, which also owns the StallDetector instance.
        /// Thus holding a Consumer refernce is safe here.
        DB::Kafka::Consumer & consumer;
        Int32 partition{-1};
        KafkaSource::Timeouts timeouts;
        Int64 recorded_last_processed_sn{0};
        Int64 recorded_latest_sn{0};
        Stopwatch timer;
        Stopwatch caught_up_timer;
        LoggerPtr logger;
    };

    void initParseUnits(size_t units_num);

    size_t parseMessage(
        const std::shared_ptr<StreamingFormatExecutor> & executor, const rd_kafka_message_t * message, MutableColumns & virtual_cols);

    size_t parseMessagesInBatch(
        const std::shared_ptr<StreamingFormatExecutor> & executor, const std::vector<StringRef> & message_refs, std::string & buf);

    void onFormatError(int64_t message_offset, Exception & ex);

    MutableColumns generateEmptyVirtualColumns() const;
    void appendVirtualColumnsRow(MutableColumns & virtual_columns, const rd_kafka_message_t  * message, size_t rows) const;
    Chunk generateOutputChunk(MutableColumns physical_columns, MutableColumns virtual_columns);

    Chunk outputParseUnit(ParseUnit & unit);

    bool tryBatchParseAndGenerate(const rd_kafka_message_t ** messages, size_t begin, size_t end, ParseUnit & unit);
    void parseAndGenerate(const rd_kafka_message_t ** messages, size_t begin, size_t end, ParseUnit & unit);

    void doCheckpoint(CheckpointContextPtr ckpt_ctx_) override;
    void doRecover(CheckpointContextPtr ckpt_ctx_) override;
    void doResetStartSN(Int64 sn) override;

    void getPhysicalHeader() override;

    Field decodeAvroKey(const rd_kafka_message_t * kmessage) const;

    Strings doFetchData(const Streaming::SequenceRange &) override;

    Chunk generateSequential();
    Chunk generateParallel();

    /// Methods for parallel parsing
    void readerThreadFunction(ThreadGroupPtr thread_group);
    void parserThreadFunction(ThreadGroupPtr thread_group, size_t unit_index, const std::vector<CopiedKafkaMessage> & messages);
    void finishParallelParsing() noexcept;

    String data_format;
    const String topic;
    const size_t partition;

    std::vector<std::function<Field(const rd_kafka_message_s *)>> virtual_col_value_functions;
    std::vector<DataTypePtr> virtual_col_types;

    bool ignore_format_errors = false;
    std::shared_ptr<KafkaSchemaRegistryForAvro> avro_key_schema_registry;

    UInt32 record_consume_batch_count = 1000;
    Int32 record_consume_timeout_ms = 100;

    Int64 offset;
    Int64 high_watermark;
    DB::Kafka::ConsumerPtr consumer;

    std::atomic_flag generate_inited;

    /// Indicates that the source has already consumed all messages it is supposed to read [for non-streaming queries].
    bool reached_the_end = false;

    std::unique_ptr<TimeBasedThrottler> watermark_error_log_throttler;

    StallDetector stall_detector;

    ExternalStreamCounterPtr external_stream_counter;

    UInt64 connection_timeout_ms;

    /// Format settings for creating format executors in parser threads
    FormatSettings format_settings;

    /// Parallel parsing config
    bool parallel_parsing_enabled = false;
    size_t parallel_parsing_threads = 4;

    std::deque<ParseUnit> processing_units;
    size_t next_output_sequence = 0;
    size_t reader_ticket_number = 0;

    std::atomic<bool> parallel_parsing_finished{false};

    /// Threads
    std::optional<ThreadPool> parser_pool;
    ThreadFromGlobalPool reader_thread;
};

}

}
