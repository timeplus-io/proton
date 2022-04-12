#pragma once

#include "Stream.h"

#include <string>
#include <functional>

#include <boost/functional/hash.hpp>
#include <fmt/format.h>

namespace nlog
{
struct StreamShard
{
    StreamShard(const std::string & stream_, const StreamID & stream_id_, int32_t shard_): stream(stream_, stream_id_), shard(shard_) { }
    StreamShard(std::string && stream_, const StreamID & stream_id_, int32_t shard_): stream(std::move(stream_), stream_id_), shard(shard_) { }
    StreamShard(const Stream & stream_, int32_t shard_): stream(stream_), shard(shard_) { }

    /// StreamShard() { }

    std::string string() const
    {
        return fmt::format("{}-{}", stream.name, shard);
    }

    bool operator==(const StreamShard & rhs) const
    {
        return stream == rhs.stream && shard == rhs.shard;
    }

    Stream stream;
    int32_t shard = 0;
};
}

template<>
struct std::hash<nlog::StreamShard>
{
    std::size_t operator()(const nlog::StreamShard & stream_shard) const noexcept
    {
        size_t v = 0;
        boost::hash_combine(v, std::hash<nlog::Stream>{}(stream_shard.stream));
        boost::hash_combine(v, std::hash<int32_t>{}(stream_shard.shard));
        return v;
    }
};
