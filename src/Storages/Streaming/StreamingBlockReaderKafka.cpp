#include "StreamingBlockReaderKafka.h"
#include "StreamShard.h"

#include <Interpreters/StorageID.h>
#include <KafkaLog/KafkaWALCommon.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int OK;
}

StreamingBlockReaderKafka::StreamingBlockReaderKafka(
    std::shared_ptr<StreamShard> stream_shard_,
    Int64 offset,
    SourceColumnsDescription::PhysicalColumnPositions column_positions,
    klog::KafkaWALSimpleConsumerPtr consumer_,
    Poco::Logger * log_)
    : stream_shard(std::move(stream_shard_))
    , schema(stream_shard->storageStream()->getInMemoryMetadataPtr()->getSampleBlock())
    , consumer(std::move(consumer_))
    , consume_ctx(toString(stream_shard->storageStream()->getStorageID().uuid), stream_shard->getShard(), offset)
    , log(log_)
{
    if (offset == nlog::LATEST_SN)
        consume_ctx.auto_offset_reset = "latest";
    else if (offset == nlog::EARLIEST_SN)
        consume_ctx.auto_offset_reset = "earliest";

    consume_ctx.enforce_offset = true;
    consume_ctx.schema_ctx.schema_provider = this;
    /// FIXME, schema version
    consume_ctx.schema_ctx.read_schema_version = 0;
    consume_ctx.schema_ctx.column_positions = std::move(column_positions);
    consumer->initTopicHandle(consume_ctx);

    const auto & positions = consume_ctx.schema_ctx.column_positions.positions;
    LOG_INFO(
        log,
        "Start streaming reading from topic={} shard={} offset={} column_positions={}",
        consume_ctx.topic,
        stream_shard->getShard(),
        offset,
        fmt::join(positions.begin(), positions.end(), ","));
}

StreamingBlockReaderKafka::~StreamingBlockReaderKafka()
{
    LOG_INFO(log, "Stop streaming reading from topic={} shard={}", consume_ctx.topic, consume_ctx.partition);

    consumer->stopConsume(consume_ctx);
}

const Block & StreamingBlockReaderKafka::getSchema(UInt16 /*schema_version*/) const
{
    return schema;
}

nlog::RecordPtrs StreamingBlockReaderKafka::read(UInt32 count, Int32 timeout_ms)
{
    auto result{consumer->consume(count, timeout_ms, consume_ctx)};
    if (result.err != ErrorCodes::OK)
    {
        LOG_ERROR(log, "Failed to consume streaming, topic={} shard={} err={}", consume_ctx.topic, consume_ctx.partition, result.err);
        return {};
    }

    return std::move(result.records);
}

std::pair<String, Int32> StreamingBlockReaderKafka::getStreamShard() const
{
    return stream_shard->getStreamShard();
}

void StreamingBlockReaderKafka::resetOffset(Int64 offset)
{
    consume_ctx.offset = offset;
}

}
