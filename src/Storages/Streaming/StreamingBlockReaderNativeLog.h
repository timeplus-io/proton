#pragma once

#include <NativeLog/Requests/FetchRequest.h>
#include <NativeLog/Record/SchemaProvider.h>
#include <NativeLog/Record/Record.h>

namespace Poco
{
class Logger;
}

namespace nlog
{
class NativeLog;
class TailCache;
}

namespace DB
{
class StreamShard;

class StreamingBlockReaderNativeLog final : nlog::SchemaProvider
{
public:
    StreamingBlockReaderNativeLog(
        std::shared_ptr<StreamShard> stream_shard_,
        Int64 sn,
        Int64 max_wait_ms,
        Int64 read_buf_size_,
        const nlog::SchemaProvider * schema_provider,
        UInt16 schema_version,
        SourceColumnsDescription::PhysicalColumnPositions column_positions_,
        Poco::Logger * logger_);

    const Block & getSchema(UInt16 /*schema_version*/) const override { return schema; }

    nlog::RecordPtrs read();

    std::pair<String, Int32> getStreamShard() const;

    /// Call this function only before read()
    void resetSequenceNumber(UInt64 sn);

private:
    nlog::RecordPtrs processCached(nlog::RecordPtrs records);

private:
    nlog::NativeLog & native_log;
    nlog::TailCache & tail_cache;

    bool inmemory = false;

    std::shared_ptr<StreamShard> stream_shard;
    Block schema;

    String ns;
    nlog::FetchRequest fetch_request;
    nlog::SchemaContext schema_ctx;

    std::vector<String> column_names;
    Poco::Logger * logger;
};

}
