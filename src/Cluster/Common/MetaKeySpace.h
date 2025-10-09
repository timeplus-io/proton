#pragma once

#include <cstdint>

namespace cluster
{
enum class MetaKeySpace : uint8_t
{
    Stream = 0xfe,
    UDF = 0xfc,
    AccessEntity = 0xfb,
    FormatSchema = 0xfa,
    Alert = 0xf9,
    Task = 0xf8,
    Database = 0xf4,
    Disk = 0xf3,
    StoragePolicy = 0xf2,

    /// Internal use
    Node = 0xdf,
    MetaAppliedSN = 0xdd,

    MaterializedViewAssignment = 0xd9,

    PendingRequest = 0xd7,
};
}
