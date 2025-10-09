#pragma once

#include <Core/Block.h>

namespace DB
{
struct BlockWithShard
{
    Block block;
    UInt32 shard;

    BlockWithShard(Block && block_, UInt32 shard_) : block(std::move(block_)), shard(shard_) { }
};

using BlocksWithShard = std::vector<BlockWithShard>;
}
