#pragma once

#include <Cluster/Common/ShardReplica.h>
#include <Cluster/Common/Stream.h>
#include <Cluster/Common/StreamShard.h>

namespace cluster
{
inline std::vector<StreamShard> streamShards(const Stream & stream, const std::vector<ShardReplica> & shard_replicas)
{
    std::vector<StreamShard> stream_shards;
    stream_shards.reserve(shard_replicas.size());

    for (const auto & shard_replica : shard_replicas)
        stream_shards.emplace_back(stream, shard_replica.shard);

    return stream_shards;
}
}
