#pragma once

#include <Core/Block.h>
#include <Core/LightChunk.h>

namespace DB
{
template <typename DataBlock>
struct DataBlockWithShard
{
    DataBlock block;
    int32_t shard;

    DataBlockWithShard(DataBlock && block_, int32_t shard_) : block(std::move(block_)), shard(shard_) { }
};

using BlockWithShard = DataBlockWithShard<Block>;
using BlocksWithShard = std::vector<BlockWithShard>;

using LightChunkWithShard = DataBlockWithShard<LightChunk>;
using LightChunksWithShard = std::vector<LightChunkWithShard>;
}

