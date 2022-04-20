#include "StreamingBlockReaderKafka.h"

#include <Interpreters/StorageID.h>
#include <KafkaLog/KafkaWALCommon.h>
#include <KafkaLog/KafkaWALSimpleConsumer.h>
#include <Storages/IStorage.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int OK;
}

StreamingBlockReaderKafka::StreamingBlockReaderKafka(
    std::shared_ptr<IStorage> storage_,
    Int32 shard_,
    Int64 offset,
    std::vector<uint16_t> column_positions,
    klog::KafkaWALSimpleConsumerPtr consumer_,
    Poco::Logger * log_)
    : storage(std::move(storage_))
    , header(storage->getInMemoryMetadataPtr()->getSampleBlock())
    , consumer(std::move(consumer_))
    , consume_ctx(toString(storage->getStorageID().uuid), shard_, offset)
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

    LOG_INFO(
        log,
        "Start streaming reading from topic={} shard={} offset={} column_positions={}",
        consume_ctx.topic,
        shard_,
        offset,
        fmt::join(consume_ctx.schema_ctx.column_positions.begin(), consume_ctx.schema_ctx.column_positions.end(), ","));
}

StreamingBlockReaderKafka::~StreamingBlockReaderKafka()
{
    LOG_INFO(log, "Stop streaming reading from topic={} shard={}", consume_ctx.topic, consume_ctx.partition);

    consumer->stopConsume(consume_ctx);
}

const Block & StreamingBlockReaderKafka::getSchema(UInt16 /*schema_version*/) const
{
    return header;
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
}
