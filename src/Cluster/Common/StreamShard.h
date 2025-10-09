#pragma once

#include <Cluster/Common/Stream.h>
#include <Cluster/Common/StreamIDShard.h>
#include <Cluster/Common/utils.h>

#include <boost/functional/hash.hpp>

#include <functional>
#include <string>

namespace cluster
{
SERDE struct StreamShard
{
    StreamShard(const std::string & ns_, const std::string & name_, const StreamID & id_, uint32_t shard_)
        : ns(ns_), name(name_), id_shard(id_, shard_)
    {
    }

    StreamShard(std::string && ns_, std::string && name_, const StreamID & id_, uint32_t shard_)
        : ns(std::move(ns_)), name(std::move(name_)), id_shard(id_, shard_)
    {
    }

    StreamShard(std::string && ns_, std::string && name_, const StreamIDShard & id_shard_)
        : ns(std::move(ns_)), name(std::move(name_)), id_shard(id_shard_)
    {
    }

    StreamShard(const Stream & stream_, uint32_t shard_) : ns(stream_.ns), name(stream_.name), id_shard(stream_.id, shard_) { }

    StreamShard(Stream && stream_, uint32_t shard_) : ns(std::move(stream_.ns)), name(std::move(stream_.name)), id_shard(stream_.id, shard_)
    {
    }

    StreamShard() = default;
    StreamShard & operator=(const StreamShard & other) = default;

    StreamShard(const StreamShard & rhs) = default;

    const auto & id() const noexcept { return id_shard.id; }
    auto shard() const noexcept { return id_shard.shard; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version);

    size_t approximateSerializedSize() const noexcept
    {
        return cluster::approximateSerializedSizeOf(ns, name) + id_shard.approximateSerializedSize();
    }

    std::string string() const;

    bool operator==(const StreamShard & rhs) const noexcept { return id_shard == rhs.id_shard && ns == rhs.ns && name == rhs.name; }

    Stream stream() const { return {ns, name, id()}; }

    std::string logName() const { return fmt::format("{}.{}.{}", ns, name, id_shard.shard); }
    std::string logNameFull() const { return fmt::format("{}.{}.{}.{}", ns, name, id_shard.shard, id_shard.idString()); }
    std::string streamLogName() const { return fmt::format("{}.{}.{}", ns, name, id_shard.idString()); }

    /// stream namespace + stream name is globally unique
    /// stream id itself is globally unique
    std::string ns;
    std::string name;

    StreamIDShard id_shard;
};
}

template <>
struct std::hash<cluster::StreamShard>
{
    std::size_t operator()(const cluster::StreamShard & stream_shard) const noexcept
    {
        return std::hash<cluster::StreamIDShard>{}(stream_shard.id_shard);
    }
};
