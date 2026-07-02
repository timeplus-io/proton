#pragma once

#include <Cluster/Common/StreamShard.h>
#include <Cluster/SchemaRecord/SchemaContext.h>
#include <Cluster/SchemaRecord/SchemaProvider.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <Cluster/SchemaRecord/SourceColumnsDescription.h>
#include <Storages/CachedSchemaProvider.h>
#include <Storages/IStorage_fwd.h>

namespace Poco
{
class Logger;
}

namespace DB
{
/// StreamingBlockReader read blocks from streaming storage
class StreamingBlockReaderBase
{
public:
    StreamingBlockReaderBase(
        const cluster::StreamShard & stream_shard_,
        const IStorage * storage,
        Block sample_block,
        UInt16 query_schema_version,
        cluster::SourceColumnsDescription::PhysicalColumnPositions column_positions,
        std::function<bool()> is_stopped_,
        LoggerPtr logger_);

    virtual ~StreamingBlockReaderBase() = default;

    virtual cluster::SchemaRecordPtrs read() = 0;
    virtual void resetSequenceNumber(Int64 sn) = 0;
    virtual Int64 fetchedSequenceNumber() const = 0;
    virtual std::pair<Int64, Int64> sequenceRange() const = 0;

    std::pair<String, Int32> getStreamShard() const;
    bool isCancelled() const noexcept;
    String description() const;

protected:
    cluster::StreamShard stream_shard;
    Block schema;
    std::function<bool()> is_stopped;
    std::unique_ptr<const cluster::SchemaContext> schema_ctx;
    std::unique_ptr<CachedSchemaProvider> schema_provider;
    LoggerPtr logger;
};

using StreamingBlockReaderPtr = std::unique_ptr<StreamingBlockReaderBase>;
}
