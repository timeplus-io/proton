#pragma once

#include <DistributedWriteAheadLog/KafkaWALContext.h>
#include <DistributedWriteAheadLog/KafkaWALSimpleConsumer.h>
#include <Interpreters/Context_fwd.h>

namespace Poco
{
    class Logger;
}

namespace DB
{

struct StorageID;

/// StreamingBlockReader read blocks from streaming storage
class StreamingBlockReader final
{
public:
    StreamingBlockReader(
        const StorageID & storage_id,
        ContextPtr context_,
        Int32 shard_,
        const DWAL::KafkaWALSimpleConsumerPtr & consumer_,
        Poco::Logger * log_);

    ~StreamingBlockReader();

    DWAL::RecordPtrs read(Int32 timeout_ms);

private:
    ContextPtr context;

    DWAL::KafkaWALSimpleConsumerPtr consumer;
    DWAL::KafkaWALContext consume_ctx;

    Poco::Logger * log;
};
}
