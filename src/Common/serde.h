#pragma once

#include <IO/VarInt.h>
#include <base/types.h>
#include <Common/VersionRevision.h>

namespace DB
{
/// REQUIRES: The object must support versioned serialization/deserialization
template <typename S, typename WB>
concept VersionedSerializable = requires(const S & s, WB && wb, VersionType version) { s.serialize(wb, version); };

template <typename S, typename RB>
concept VersionedDeserializable = requires(S && s, RB && rb, VersionType version) { s.deserialize(rb, version); };

template <typename T>
concept HasGetVersionFromRevision = requires(T && a) {
    {
        a.getVersionFromRevision(ProtonRevision::getVersionRevision())
    } -> std::convertible_to<VersionType>;
};

template <typename WB, VersionedSerializable<WB> S>
void ALWAYS_INLINE serialize(const S & s, WB && wb, std::optional<VersionType> version = std::nullopt)
{
    if (version.has_value())
    {
        s.serialize(wb, version.value());
    }
    else
    {
        /// When version is not required, by default use `Revision` for version management, unless it has its own versions
        if constexpr (HasGetVersionFromRevision<S>)
            s.serialize(wb, static_cast<VersionType>(s.getVersionFromRevision(ProtonRevision::getVersionRevision())));
        else
            s.serialize(wb, static_cast<VersionType>(ProtonRevision::getVersionRevision()));
    }
}

template <typename RB, VersionedDeserializable<RB> S>
void ALWAYS_INLINE deserialize(S & s, RB && rb, std::optional<VersionType> version = std::nullopt)
{
    if (version.has_value())
    {
        s.deserialize(rb, version.value());
    }
    else
    {
        /// When version is not required, by default use `Revision` for version management, unless it has its own versions
        if constexpr (HasGetVersionFromRevision<S>)
            s.deserialize(rb, static_cast<VersionType>(s.getVersionFromRevision(ProtonRevision::getVersionRevision())));
        else
            s.deserialize(rb, static_cast<VersionType>(ProtonRevision::getVersionRevision()));
    }
}

/// macro tag to indicate the data members or struct or class will
/// be serialized / deserialized via network or file system IO.
/// Hence, data structure versioning / backward / forward compatibility
/// are concerns
#define SERDE
#define NO_SERDE
}