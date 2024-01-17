#pragma once

#include <IO/VarInt.h>
#include <base/types.h>
#include <Common/VersionRevision.h>

namespace DB
{
/// REQUIRES: The object must support versioned serialization/deserialization
template <typename S, typename WB, typename... Args>
concept VersionedSerializable
    = requires(const S & s, WB & wb, VersionType version, Args &&... args) { s.serialize(wb, version, std::forward<Args>(args)...); };

template <typename S, typename RB, typename... Args>
concept VersionedDeserializable
    = requires(S & s, RB & rb, VersionType version, Args &&... args) { s.deserialize(rb, version, std::forward<Args>(args)...); };

template <typename WB, typename... Args, VersionedSerializable<WB, Args...> S>
void ALWAYS_INLINE serialize(const S & s, WB & wb, VersionType version, Args &&... args)
{
    s.serialize(wb, version, std::forward<Args>(args)...);
}

template <typename RB, typename... Args, VersionedDeserializable<RB, Args...> S>
void ALWAYS_INLINE deserialize(S & s, RB & rb, VersionType version, Args &&... args)
{
    s.deserialize(rb, version, std::forward<Args>(args)...);
}

/// macro tag to indicate the data members or struct or class will
/// be serialized / deserialized via network or file system IO.
/// Hence, data structure versioning / backward / forward compatibility
/// are concerns
#define SERDE
#define NO_SERDE
}