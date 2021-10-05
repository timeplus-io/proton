#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <DistributedWriteAheadLog/Results.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{
class Context;
class Block;
class StorageDistributedMergeTree;

struct BlockWithShard
{
    Block block;
    size_t shard;

    BlockWithShard(Block && block_, size_t shard_) : block(std::move(block_)), shard(shard_) { }
};

using BlocksWithShard = std::vector<BlockWithShard>;

class DistributedMergeTreeSink final : public SinkToStorage
{
public:
    DistributedMergeTreeSink(
        StorageDistributedMergeTree & storage_, const StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_);
    ~DistributedMergeTreeSink() override = default;

    void consume(Chunk chunk) override;
    void onFinish() override;
    String getName() const override { return "DistributedMergeTreeSink"; }

private:
    BlocksWithShard shardBlock(const Block & block) const;
    BlocksWithShard doShardBlock(const Block & block) const;
    String getIngestMode() const;

private:
    void writeCallback(const DWAL::AppendResult & result);

    static void writeCallback(const DWAL::AppendResult & result, void * data);

private:
    StorageDistributedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;

    /// For writeCallback
    std::atomic_uint32_t committed = 0;
    std::atomic_uint32_t outstanding = 0;
    std::atomic_int32_t errcode = 0;
};

}
