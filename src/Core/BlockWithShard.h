#pragma once

#include <Core/Block.h>

namespace DB
{
struct BlockWithShard
{
    Block block;
    int32_t shard;

    BlockWithShard(Block && block_, int32_t shard_) : block(std::move(block_)), shard(shard_) { }
};

using BlocksWithShard = std::vector<BlockWithShard>;
}

