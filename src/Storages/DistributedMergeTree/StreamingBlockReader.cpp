#include "StreamingBlockReader.h"

#include <DistributedWALClient/KafkaWALCommon.h>
#include <DistributedWALClient/KafkaWALSimpleConsumer.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int OK;
}

StreamingBlockReader::StreamingBlockReader(
    std::shared_ptr<IStorage> storage_,
    Int32 shard_,
    Int64 offset,
    std::vector<size_t> column_positions,
    const DWAL::KafkaWALSimpleConsumerPtr & consumer_,
    Poco::Logger * log_)
    : storage(std::move(storage_))
    , header(storage->getInMemoryMetadataPtr()->getSampleBlock())
    , consumer(consumer_)
    , consume_ctx(
          DWAL::escapeDWALName(storage->getStorageID().getDatabaseName(), storage->getStorageID().getTableName()),
          shard_,
          Int64{-1} /* latest */)
    , log(log_)
{
    if (offset == -1)
        consume_ctx.auto_offset_reset = "latest";
    else if (offset == -2)
        consume_ctx.auto_offset_reset = "earliest";

    consume_ctx.offset = offset;
    consume_ctx.enforce_offset = true;
    consume_ctx.schema_ctx.schema_provider = this;
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

StreamingBlockReader::~StreamingBlockReader()
{
    LOG_INFO(log, "Stop streaming reading from topic={} shard={}", consume_ctx.topic, consume_ctx.partition);

    consumer->stopConsume(consume_ctx);
}

const Block & StreamingBlockReader::getSchema(UInt16 /*schema_version*/) const
{
    return header;
}

DWAL::RecordPtrs StreamingBlockReader::read(UInt32 count, Int32 timeout_ms)
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
