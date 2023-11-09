#pragma once

#include <random>

#include <Core/BlockWithShard.h>
#include <Formats/FormatFactory.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Kafka/WriteBufferFromKafka.h>
#include <Common/ThreadPool.h>

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

    IColumn::Selector createSelector(const Block & block, Int32 partition_cnt) const;

    ExpressionActionsPtr partitioning_expr;
    String partitioning_key_column_name;
    bool random_partitioning = false;
    mutable std::minstd_rand rand;
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
