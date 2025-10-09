#include <Core/Field.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Storages/Stream/StreamingBlockReaderBase.h>

namespace DB
{
StreamingBlockReaderBase::StreamingBlockReaderBase(
    const cluster::StreamShard & stream_shard_,
    const IStorage * storage,
    Block sample_block,
    UInt16 query_schema_version,
    cluster::SourceColumnsDescription::PhysicalColumnPositions column_positions,
    std::function<bool()> is_stopped_,
    LoggerPtr logger_)
    : stream_shard(stream_shard_)
    , schema(std::move(sample_block))
    , is_stopped(std::move(is_stopped_))
    , schema_provider(std::make_unique<CachedSchemaProvider>(storage))
    , logger(logger_)
{
    schema_ctx = std::make_unique<cluster::SchemaContext>(
        *schema_provider, stream_shard.name, stream_shard.id(), query_schema_version, std::move(column_positions));
}

std::pair<String, Int32> StreamingBlockReaderBase::getStreamShard() const
{
    return {toString(stream_shard.id()), stream_shard.shard()};
}

bool StreamingBlockReaderBase::isCancelled() const noexcept
{
    return is_stopped();
}

String StreamingBlockReaderBase::description() const
{
    return fmt::format("stream={}.{},shard={}", stream_shard.ns, stream_shard.name, stream_shard.shard());
}
}
