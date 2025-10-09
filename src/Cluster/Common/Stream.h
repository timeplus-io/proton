#pragma once

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/StreamID.h>

#include <Cluster/Common/SerdeTag.h>

#include <functional>
#include <string>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster
{

SERDE struct Stream
{
    Stream() = default;

    Stream(std::string && ns_, std::string && name_, const StreamID & id_) : ns(std::move(ns_)), name(std::move(name_)), id(id_) { }

    Stream(const std::string & ns_, const std::string & name_, const StreamID & id_) : ns(ns_), name(name_), id(id_) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version);

    size_t approximateSerializedSize() const noexcept;

    std::string string() const;

    bool isValid() const noexcept { return (!ns.empty() && !name.empty()) || (id != Nulls::NullStreamID); }

    bool operator==(const Stream & rhs) const noexcept
    {
        return id != Nulls::NullStreamID ? (id == rhs.id) : (ns == rhs.ns && name == rhs.name);
    }

    bool operator!=(const Stream & rhs) const noexcept { return !(*this == rhs); }

    /// stream namespace + stream name is globally unique
    /// stream id itself is globally unique
    std::string ns;
    std::string name;
    StreamID id;
};
}

template <>
struct std::hash<cluster::Stream>
{
    std::size_t operator()(const cluster::Stream & stream) const noexcept
    {
        assert(stream.id != cluster::Nulls::NullStreamID);
        return std::hash<cluster::StreamID>{}(stream.id);
    }
};
