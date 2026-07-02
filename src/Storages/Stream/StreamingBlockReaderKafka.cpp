#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamShardStore.h>
#include <Storages/Stream/StreamingBlockReaderKafka.h>

#include <Cluster/Common/Constants.h>
#include <Cluster/KafkaLog/KafkaWALCommon.h>
#include <Interpreters/StorageID.h>
#include <Common/logger_useful.h>
#include <fmt/ranges.h>

namespace DB
{
namespace ErrorCodes
{
const extern int OK;
}

StreamingBlockReaderKafka::StreamingBlockReaderKafka(
    Int64 offset,
    size_t max_wait_ms_,
    UInt32 max_batch_count_,
    const cluster::StreamShard & stream_shard_,
    const IStorage * storage,
    Block sample_block,
    UInt16 query_schema_version,
    cluster::SourceColumnsDescription::PhysicalColumnPositions column_positions,
    cluster::klog::KafkaWALSimpleConsumerPtr consumer_,
    std::function<bool()> is_stopped_,
    LoggerPtr logger_)
    : StreamingBlockReaderBase(
          stream_shard_,
          storage,
          std::move(sample_block),
          query_schema_version,
          std::move(column_positions),
          std::move(is_stopped_),
          logger_)
    , consumer(std::move(consumer_))
    , consume_ctx(toString(stream_shard.id()), stream_shard.shard(), offset)
    , max_wait_ms(max_wait_ms_)
    , max_batch_count(max_batch_count_)
{
    if (offset == cluster::Constants::LatestSN)
        consume_ctx.auto_offset_reset = "latest";
    else if (offset == cluster::Constants::EarliestSN)
        consume_ctx.auto_offset_reset = "earliest";

    consume_ctx.enforce_offset = true;
    consume_ctx.schema_ctx = *schema_ctx;
    consumer->initTopicHandle(consume_ctx);

    const auto & positions = consume_ctx.schema_ctx.column_positions_requested.positions;

    LOG_INFO(
        logger,
        "Start streaming reading from topic={} partition={} offset={} column_positions={}",
        consume_ctx.topic,
        stream_shard.shard(),
        offset,
        fmt::join(positions.begin(), positions.end(), ","));
}

StreamingBlockReaderKafka::~StreamingBlockReaderKafka()
{
    LOG_INFO(logger, "Stop streaming reading from topic={} shard={}", consume_ctx.topic, consume_ctx.partition);

    consumer->stopConsume(consume_ctx);
}

cluster::SchemaRecordPtrs StreamingBlockReaderKafka::read()
{
    auto result{consumer->consume(max_batch_count, static_cast<int32_t>(max_wait_ms), consume_ctx)};
    if (result.err != ErrorCodes::OK)
    {
        LOG_ERROR(logger, "Failed to consume streaming, topic={} shard={} err={}", consume_ctx.topic, consume_ctx.partition, result.err);
        return {};
    }

    return std::move(result.records);
}

void StreamingBlockReaderKafka::resetSequenceNumber(Int64 offset)
{
    consume_ctx.offset = offset;
}

}
