#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamSink.h>

#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace
{
IngestMode getIngestModeForSink(const ContextPtr & context, const StorageStream & storage)
{
    if (auto current_ingest_mode = context->getIngestMode(); current_ingest_mode)
        return *current_ingest_mode;

    return storage.ingestMode();
}
}

StreamSink::StreamSink(std::shared_ptr<StorageStream> storage_, StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_)
    : StreamSinkBase(
          storage_,
          std::move(metadata_snapshot_),
          getIngestModeForSink(query_context_, *storage_),
          storage_->getShards(),
          std::move(query_context_),
          getLogger("StreamSink"),
          ProcessorID::StreamSinkID)
    , storage_stream(storage_.get())
{
}

BlocksWithShard StreamSink::shardBlock(Block block) const
{
    if (storage_stream->shards > 1 && storage_stream->sharding_key_expr && !storage_stream->rand_sharding_key)
    {
        auto selector = storage_stream->createSelector(block);
        return chunk_splitter.splitBlock(std::move(block), storage_stream->shards, selector);
    }

    auto shard = storage_stream->getNextShardIndex();
    return chunk_splitter.splitBlockForSingleShard(std::move(block), shard);
}

cluster::CallResultV<int64_t> StreamSink::appendLog(cluster::SchemaRecordPtr & record, int64_t timeout_ms)
{
    auto errcode = storage_stream->append(record, ingest_mode, {}, {}, timeout_ms);
    if (errcode != DB::ErrorCodes::OK)
        return cluster::CallResultV<int64_t>{errcode, std::string{DB::ErrorCodes::getName(errcode)}};

    return cluster::CallResultV<int64_t>{int64_t{0}};
}

}

