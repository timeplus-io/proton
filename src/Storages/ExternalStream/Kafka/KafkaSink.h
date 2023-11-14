#pragma once

#include <Core/BlockWithShard.h>
#include <Formats/FormatFactory.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Kafka/WriteBufferFromKafka.h>
#include <Common/ThreadPool.h>

#include <random>

namespace Poco
{
class Logger;
}

namespace DB
{

namespace KafkaStream
{
/// Partition Chunk's to paritions (i.e. shards) by the partitioning expression.
class ChunkPartitioner
{
public:
    ChunkPartitioner(ContextPtr context, const Block & header, const ASTPtr & partitioning_expr_ast);
    BlocksWithShard partition(Block block, Int32 partition_cnt) const;

private:
    Int32 getNextShardIndex(Int32 partition_cnt) const noexcept { return static_cast<Int32>(rand()) % partition_cnt; }

    BlocksWithShard doParition(Block block, Int32 partition_cnt) const;

    IColumn::Selector createSelector(Block block, Int32 partition_cnt) const;

    static inline std::minstd_rand rand{std::random_device()()};

    ExpressionActionsPtr partitioning_expr;
    String partitioning_key_column_name;
    bool random_partitioning = false;
};
}

class KafkaSink final : public SinkToStorage
{
public:
    KafkaSink(const Kafka * kafka, const Block & header, ContextPtr context, Int32 initial_partition_cnt, Poco::Logger * log_);
    ~KafkaSink() override;

    String getName() const override { return "KafkaSink"; }

    void consume(Chunk chunk) override;
    void onFinish() override;
    void checkpoint(CheckpointContextPtr) override;

private:
    static void onMessageDelivery(rd_kafka_t * /* producer */, const rd_kafka_message_t * msg, void * opaque)
    {
        static_cast<KafkaSink *>(opaque)->wb->onMessageDelivery(msg);
    }

    static int32_t onPartitioning(
        const rd_kafka_topic_t * /*rkt*/,
        const void * /*keydata*/,
        size_t /*keylen*/,
        int32_t partition_count,
        void * rkt_opaque,
        void * msg_opaque)
    {
        /// update partition count
        auto * sink = static_cast<KafkaSink *>(rkt_opaque);
        sink->partition_cnt = partition_count;

        auto partition_id_ptr = reinterpret_cast<std::uintptr_t>(msg_opaque);
        auto parition_id = static_cast<Int32>(partition_id_ptr);
        /// This should not really happen because Kafka does not support reducing partitions.
        /// However, KIP-694 is currently under discussion, so this might heppen in the future.
        if (parition_id >= partition_count)
            parition_id = partition_count - 1;
        return parition_id;
    }

    static const int POLL_TIMEOUT_MS = 500;

    klog::KafkaPtr producer;
    klog::KTopicPtr topic;
    std::unique_ptr<WriteBufferFromKafka> wb;
    OutputFormatPtr writer;
    ThreadPool polling_threads;
    std::atomic_flag is_finished;
    Int32 partition_cnt;
    std::unique_ptr<KafkaStream::ChunkPartitioner> partitioner;

    Poco::Logger * log;
};
}
