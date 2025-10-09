#pragma once

#include <Cluster/Common/SerdeTag.h>

#include <cstdint>
#include <string>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace cluster
{
/// Server version
SERDE struct Version
{
    void serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t /*version*/);

    size_t approximateSerializedSize() const noexcept { return sizeof(Version); }

    std::string string() const;

    bool operator==(Version rhs) const noexcept
    {
        return major == rhs.major && minor == rhs.minor && patch == rhs.patch && internal == rhs.internal;
    }

    bool operator<(Version rhs) const noexcept
    {
        return std::tie(major, minor, patch, internal) < std::tie(rhs.major, rhs.minor, rhs.patch, rhs.internal);
    }

    uint16_t major = 0;
    uint16_t minor = 0;
    uint16_t patch = 0;
    /// The internal version which is used to migrations during dev cycle
    uint16_t internal = 0;
};
}
