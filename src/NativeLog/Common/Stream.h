#pragma once

#include <base/UUID.h>

#include <string>
#include <functional>

namespace nlog
{
using StreamID = DB::UUID;

struct Stream
{
    Stream(const std::string & name_, const StreamID & id_) : name(name_), id(id_) { }

    bool operator==(const Stream & rhs) const
    {
        return id == rhs.id;
    }

    std::string name;
    StreamID id;
};
}

template<>
struct std::hash<nlog::Stream>
{
    std::size_t operator()(const nlog::Stream & stream) const noexcept
    {
        return std::hash<DB::UUID>{}(stream.id);
    }
};
