#pragma once

#include <cstdint>

namespace cluster
{
/// Shards are still needed for parallelism in  mode
/// No replica or type concepts needed for
struct Shard
{
    uint32_t shard = 0; /// Shard ID - still needed for parallel processing
};

/// Compatibility alias during transition
using ShardReplica = Shard;
}
