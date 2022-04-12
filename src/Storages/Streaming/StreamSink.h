#pragma once

#include "IngestMode.h"

#include <KafkaLog/Results.h>
#include <NativeLog/Requests/AppendRequest.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace nlog
{
class NativeLog;
}

namespace DB
{
class Context;
class Block;
class StorageStream;

struct BlockWithShard
{
    Block block;
    size_t shard;

    BlockWithShard(Block && block_, size_t shard_) : block(std::move(block_)), shard(shard_) { }
};

using BlocksWithShard = std::vector<BlockWithShard>;

class StreamSink final : public SinkToStorage
{
public:
    StreamSink(StorageStream & storage_, const StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_);
    ~StreamSink() override = default;

    void consume(Chunk chunk) override;
    void onFinish() override;
    String getName() const override { return "StreamSink"; }

private:
    BlocksWithShard shardBlock(Block block) const;
    BlocksWithShard doShardBlock(Block block) const;
    IngestMode getIngestMode() const;

private:
    void appendToKafka(nlog::RecordPtr & record, IngestMode ingest_mode);
    void appendToNativeLog(nlog::RecordPtr & record, IngestMode ingest_mode);

    void writeCallback(const klog::AppendResult & result);
    static void writeCallback(const klog::AppendResult & result, void * data);

private:
    StorageStream & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;

    StorageID storage_id;
    nlog::AppendRequest request;
    nlog::NativeLog * native_log = nullptr;

    std::vector<UInt16> column_positions;

    /// For writeCallback
    std::atomic_uint64_t committed = 0;
    std::atomic_uint64_t outstanding = 0;
    std::atomic_int32_t errcode = 0;
};

}
