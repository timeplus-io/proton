#pragma once

namespace DB
{
struct ShardAppliedSequence
{
    ShardAppliedSequence(uint32_t shard_, int64_t sn_) : shard(shard_), sn(sn_) { }

    uint32_t shard;

    /// Applied sn
    int64_t sn;
};

}
