#pragma once

#include <Cluster/Common/StreamShard.h>

namespace cluster::nlog
{
struct StreamShardSequence
{
    StreamShardSequence(const StreamShard & stream_shard_, int64_t sn_) : stream_shard(stream_shard_), sn(sn_) { }

    StreamShard stream_shard;
    int64_t sn;
};
}
