#include "StreamingBlockReader.h"

#include <DistributedWriteAheadLog/KafkaWALCommon.h>
#include <DistributedWriteAheadLog/KafkaWALSimpleConsumer.h>
#include <Interpreters/StorageID.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int OK;
}

StreamingBlockReader::StreamingBlockReader(
    const StorageID & storage_id, ContextPtr context_, Int32 shard_, Int64 offset, const DWAL::KafkaWALSimpleConsumerPtr & consumer_, Poco::Logger * log_)
    : context(context_)
    , consumer(consumer_)
    , consume_ctx(DWAL::escapeDWALName(storage_id.getDatabaseName(), storage_id.getTableName()), shard_, Int64{-1} /* latest */)
    , log(log_)
{
    if (offset == -1)
        consume_ctx.auto_offset_reset = "latest";
    else if (offset == -2)
        consume_ctx.auto_offset_reset = "earliest";
    else
    {
        consume_ctx.offset = offset;
        consume_ctx.enforce_offset = true;
    }
    consumer->initTopicHandle(consume_ctx);

    LOG_INFO(log, "Start streaming reading from topic={} shard={} offset={}", consume_ctx.topic, shard_, offset);
}

StreamingBlockReader::~StreamingBlockReader()
{
    LOG_INFO(log, "Stop streaming reading from topic={} shard={}", consume_ctx.topic, consume_ctx.partition);

    consumer->stopConsume(consume_ctx);
}

DWAL::RecordPtrs StreamingBlockReader::read(Int32 timeout_ms)
{
    auto result{consumer->consume(10000, timeout_ms, consume_ctx)};
    if (result.err != ErrorCodes::OK)
    {
        LOG_ERROR(log, "Failed to consume streaming, topic={} shard={} err={}", consume_ctx.topic, consume_ctx.partition, result.err);
        return {};
    }

    return std::move(result.records);
}
}
