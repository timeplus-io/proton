#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DistributedWriteAheadLog/IDistributedWriteAheadLog.h>
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

class DistributedMergeTreeBlockOutputStream final : public IBlockOutputStream
{
public:
    DistributedMergeTreeBlockOutputStream(
        StorageDistributedMergeTree & storage_, const StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_);


    Block getHeader() const override;
    void write(const Block & block) override;
    void flush() override;

private:
    BlocksWithShard shardBlock(const Block & block) const;
    BlocksWithShard doShardBlock(const Block & block) const;

private:
    void writeCallback(const IDistributedWriteAheadLog::AppendResult & result);

    static void writeCallback(const IDistributedWriteAheadLog::AppendResult & result, void * data);

private:
    StorageDistributedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;

    /// for writeCallback
    std::atomic_uint32_t committed = 0;
    std::atomic_uint32_t outstanding = 0;
    std::atomic_int32_t err = 0;
};

}
