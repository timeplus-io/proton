#pragma once

#include <Cluster/KafkaLog/KafkaWALContext.h>
#include <Cluster/KafkaLog/KafkaWALSimpleConsumer.h>
#include <Storages/Stream/StreamingBlockReaderBase.h>

namespace DB
{
class IStreamShardStore;

class StreamingBlockReaderKafka final : public StreamingBlockReaderBase
{
public:
    StreamingBlockReaderKafka(
        Int64 offset,
        size_t max_wait_ms_,
        UInt32 max_batch_count_,
        const cluster::StreamShard & stream_shard_,
        const IStorage * storage,
        Block sample_block,
        UInt16 query_schema_version,
        cluster::SourceColumnsDescription::PhysicalColumnPositions column_positions_,
        cluster::klog::KafkaWALSimpleConsumerPtr consumer_,
        std::function<bool()> is_stopped_,
        LoggerPtr logger_);

    ~StreamingBlockReaderKafka() override;

    cluster::SchemaRecordPtrs read() override;

    std::pair<Int64, Int64> sequenceRange() const override { return {0, 0}; }

    /// Call this function only before read()
    void resetSequenceNumber(Int64 offset) override;

    Int64 fetchedSequenceNumber() const override { return consume_ctx.offset - 1; }

private:
    cluster::klog::KafkaWALSimpleConsumerPtr consumer;
    cluster::klog::KafkaWALContext consume_ctx;

    size_t max_wait_ms;
    UInt32 max_batch_count;
};
}
