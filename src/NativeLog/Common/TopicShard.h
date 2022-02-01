#pragma once

#include <string>
#include <functional>
#include <boost/functional/hash.hpp>

namespace nlog
{
struct TopicShard
{
    TopicShard(const std::string & topic_, uint32_t shard_): topic(topic_), shard(shard_) { }
    TopicShard(std::string && topic_, uint32_t shard_): topic(std::move(topic_)), shard(shard_) { }
    TopicShard() { }

    std::string string() const
    {
        return topic + "-" + std::to_string(shard);
    }

    bool operator==(const TopicShard & rhs) const
    {
        return topic == rhs.topic && shard == rhs.shard;
    }

    std::string topic;
    uint32_t shard = 0;
};
}

template<>
struct std::hash<nlog::TopicShard>
{
    std::size_t operator()(const nlog::TopicShard & topic_shard) const noexcept
    {
        size_t v = 0;
        boost::hash_combine(v, std::hash<std::string>{}(topic_shard.topic));
        boost::hash_combine(v, std::hash<uint32_t>{}(topic_shard.shard));
        return v;
    }
};
