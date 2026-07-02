#pragma once

#include <Cluster/Base/ByteVector.h>
#include <Core/BlockWithShard.h>
#include <Formats/FormatFactory.h>
#include <Processors/Executors/MessageQueueFormatExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>

#include <rdkafka.h>


namespace DB::ExternalStream
{

/// Shard Chunk's to shards (or partitions in Kafka's term) by the sharding expression.
class ChunkSharder
{
public:
    ChunkSharder(ExpressionActionsPtr sharding_expr_, const String & column_name);
    ChunkSharder();

    BlocksWithShard shard(Block block, UInt32 shard_cnt);

private:
    UInt32 getNextShardIndex([[maybe_unused]] UInt32 shard_cnt) noexcept { return RD_KAFKA_PARTITION_UA; }

    BlocksWithShard doSharding(Block block, UInt32 shard_cnt) const;

    IColumn::Selector createSelector(Block block, UInt32 shard_cnt) const;

    ExpressionActionsPtr sharding_expr;
    String sharding_key_column_name;
    bool random_sharding = false;
    [[maybe_unused]] UInt32 next_shard = 0;
};

class KafkaSink final : public SinkToStorage
{
public:
    /// Callback for Kafka message delivery report
    static void onMessageDelivery(rd_kafka_t * /* producer */, const rd_kafka_message_t * msg, void * /*opaque*/);

    KafkaSink(
        Kafka & kafka,
        const Block & header,
        DB::Kafka::ProducerPtr producer_,
        UInt64 connection_timeout_ms_,
        bool refresh_topic_partitions_,
        ExternalStreamCounterPtr external_stream_counter_,
        LoggerPtr logger_,
        ContextPtr context);
    ~KafkaSink() override;

    String getName() const override { return "KafkaSink"; }

    void consume(Chunk chunk) override;
    void onFinish() override;
    void doCheckpoint(CheckpointContextPtr) override;

private:
    void onMessageDelivery(rd_kafka_resp_err_t err);
    void sendMessage(const String & message, ColumnPtr ts_column, ColumnPtr headers_column, ColumnPtr key_column);

    /// the number of acknowledgement has been received so far for the current checkpoint period
    size_t acked() const noexcept { return state.acked; }
    /// the number of errors has been received so far for the current checkpoint period
    size_t errorCount() const noexcept { return state.error_count; }
    /// the number of outstanding messages for the current checkpoint period
    size_t outstanding() const noexcept { return state.outstanding; }
    /// the last error code received from delivery report callback
    rd_kafka_resp_err_t lastSeenError() const { return static_cast<rd_kafka_resp_err_t>(state.last_error_code.load()); }
    /// check if there are no more outstanding produce requests (i.e. delivery reports have been receieved
    /// for all out-go messages, regardless if a message is successfully delivered or not)
    size_t outstandingMessages() const noexcept { return state.outstanding - (state.acked + state.error_count); }

    DB::Kafka::ProducerPtr producer;

    UInt64 connection_timeout_ms{0};
    UInt64 flush_timeout_ms{0};
    bool refresh_topic_partitions{false};
    UInt64 checkpoint_timeout_ms{0};
    Int32 partition_cnt{0};
    bool one_message_per_row{false};
    Int32 topic_refresh_interval_ms{0};

    std::atomic_flag is_finished{false};

    std::unique_ptr<MessageQueueFormatExecutor> format_executor;
    std::unique_ptr<ChunkSharder> partitioner;

    std::optional<size_t> ts_column_pos;
    std::optional<size_t> msg_key_column_pos;
    std::optional<size_t> headers_column_pos;

    /// For constructing the message batch
    [[maybe_unused]] UInt64 rows_in_current_message{0};
    size_t current_batch_row{0};
    Int32 next_partition{0};

    Stopwatch metadata_refresh_stopwatch;

    struct State
    {
        std::atomic_size_t outstanding{0};
        std::atomic_size_t acked{0};
        std::atomic_size_t error_count{0};
        std::atomic_int32_t last_error_code{0};

        /// allows to reset the state after each checkpoint
        void reset();
    };

    State state;

    ExternalStreamCounterPtr external_stream_counter;
    LoggerPtr logger;
};

}
