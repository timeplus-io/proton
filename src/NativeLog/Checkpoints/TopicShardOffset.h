#pragma once

#include <NativeLog/Common/TopicShard.h>

namespace nlog
{
struct TopicShardOffset
{
    TopicShardOffset() { }
    TopicShardOffset(const TopicShard & topic_shard_, int64_t offset_) : topic_shard(topic_shard_), offset(offset_) { }

    TopicShard topic_shard;
    int64_t offset;
};
}
