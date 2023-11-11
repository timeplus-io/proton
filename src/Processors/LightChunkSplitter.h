#pragma once

#include <Processors/Chunk.h>

#include <vector>

namespace DB
{
struct ShardChunk
{
    ShardChunk(UInt16 shard_, Chunk && chunk_) : shard(shard_), chunk(std::move(chunk_)) { }
    ShardChunk(ShardChunk && other) noexcept : shard(other.shard), chunk(std::move(other.chunk)) { }

    UInt16 shard;
    Chunk chunk;
};

using ShardChunks = std::vector<ShardChunk>;

/// Split Chunk into max number of shards via keys columns
class LightChunkSplitter final
{
public:
    explicit LightChunkSplitter(std::vector<size_t> key_column_positions_, UInt16 total_shards_);

    ShardChunks split(Chunk & chunk) const;

    const auto & keyColumnPositions() const noexcept { return key_column_positions; }

private:
    UInt16 total_shards;
    std::vector<size_t> key_column_positions;
};

}
