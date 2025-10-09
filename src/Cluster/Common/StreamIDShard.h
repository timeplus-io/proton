#pragma once

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/StreamID.h>

#include <Cluster/Common/SerdeTag.h>

#include <boost/functional/hash.hpp>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster
{
SERDE struct StreamIDShard
{
    StreamIDShard(const StreamID & id_, uint32_t shard_) : id(id_), shard(shard_) { }

    StreamIDShard() = default;

    void serialize(DB::WriteBuffer & wb, uint16_t version) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version);
    size_t approximateSerializedSize() const noexcept;
    std::string string() const;
    std::string idString() const;

    bool operator==(const StreamIDShard & rhs) const noexcept { return id == rhs.id && shard == rhs.shard; }
    bool operator!=(const StreamIDShard & rhs) const noexcept { return !(*this == rhs); }
    bool operator<(const StreamIDShard & rhs) const noexcept { return std::tie(id, shard) < std::tie(rhs.id, rhs.shard); }

    StreamID id = Nulls::NullStreamID;
    uint32_t shard = 0;
};
}

template <>
struct std::hash<cluster::StreamIDShard>
{
    std::size_t operator()(const cluster::StreamIDShard & stream_id_shard) const noexcept
    {
        size_t v = 0;
        boost::hash_combine(v, std::hash<cluster::StreamID>{}(stream_id_shard.id));
        boost::hash_combine(v, std::hash<uint32_t>{}(stream_id_shard.shard));
        return v;
    }
};
