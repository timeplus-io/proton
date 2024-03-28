#pragma once

#include <Core/DataBlockWithShard.h>
#include <Formats/FormatFactory.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Kafka/WriteBufferFromKafkaSink.h>
#include <Common/ThreadPool.h>

namespace DB
{

namespace KafkaStream
{

/// Shard Chunk's to shards (or partitions in Kafka's term) by the sharding expression.
class ChunkSharder
{
public:
    ChunkSharder(ExpressionActionsPtr sharding_expr_, const String & column_name);
    ChunkSharder();

    BlocksWithShard shard(Block block, Int32 shard_cnt) const;

private:
    Int32 getNextShardIndex(Int32 /*shard_cnt*/) const noexcept
    {
        /// let librdkafka decides
        return RD_KAFKA_PARTITION_UA;
    }

    BlocksWithShard doSharding(Block block, Int32 shard_cnt) const;

    IColumn::Selector createSelector(Block block, Int32 shard_cnt) const;

    ExpressionActionsPtr sharding_expr;
    String sharding_key_column_name;
    bool random_sharding = false;
};

}

class KafkaSink final : public SinkToStorage
{
public:
    /// Callback for Kafka message delivery report
    static void onMessageDelivery(rd_kafka_t * /* producer */, const rd_kafka_message_t * msg, void *  /*opaque*/);

    KafkaSink(
        Kafka & kafka,
        const Block & header,
        const ASTPtr & message_key,
        ExternalStreamCounterPtr external_stream_counter_,
        ContextPtr context);
    ~KafkaSink() override;

    String getName() const override { return "KafkaSink"; }

    void consume(Chunk chunk) override;
    void onFinish() override;
    void checkpoint(CheckpointContextPtr) override;

private:
    // void onMessageDelivery(const rd_kafka_message_t * msg);
    void onMessageDelivery(rd_kafka_resp_err_t err);
    void addMessageToBatch(char * pos, size_t len);

    /// the number of acknowledgement has been received so far for the current checkpoint period
    size_t acked() const noexcept { return state.acked; }
    /// the number of errors has been received so far for the current checkpoint period
    size_t errorCount() const noexcept { return state.error_count; }
    /// the number of outstanding messages for the current checkpoint period
    size_t outstandings() const noexcept { return state.outstandings; }
    /// the last error code received from delivery report callback
    rd_kafka_resp_err_t lastSeenError() const { return static_cast<rd_kafka_resp_err_t>(state.last_error_code.load()); }
    /// check if there are no more outstandings (i.e. delivery reports have been recieved
    /// for all out-go messages, regardless if a message is successfully delivered or not)
    size_t outstandingMessages() const noexcept { return state.outstandings - (state.acked + state.error_count); }

    RdKafka::Producer & producer;
    RdKafka::Topic & topic;
    // std::unique_ptr<RdKafka::Topic> topic;

    Int32 partition_cnt {0};
    bool one_message_per_row {false};
    Int32 topic_refresh_interval_ms = 0;

    ThreadPool background_jobs {1};
    std::atomic_flag is_finished {false};

    std::unique_ptr<WriteBufferFromKafkaSink> wb;
    OutputFormatPtr writer;
    std::unique_ptr<KafkaStream::ChunkSharder> partitioner;

    ExpressionActionsPtr message_key_expr;
    String message_key_column_name;

    /// For constructing the message batch
    std::vector<rd_kafka_message_t> current_batch;
    std::vector<nlog::ByteVector> batch_payload;
    std::vector<StringRef> keys_for_current_batch;
    size_t current_batch_row {0};
    Int32 next_partition {0};

    struct State
    {
        std::atomic_size_t outstandings {0};
        std::atomic_size_t acked {0};
        std::atomic_size_t error_count {0};
        std::atomic_int32_t last_error_code {0};

        /// allows to reset the state after each checkpoint
        void reset();
    };

    State state;

    ExternalStreamCounterPtr external_stream_counter;
    Poco::Logger * logger;
};

}
