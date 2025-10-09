#pragma once

#include <base/UUID.h>

#include <limits>

namespace cluster
{
struct Nulls
{
    static constexpr uint16_t NullVersion = 0;
    /// In  mode, epoch is always 1, but keep NullEpoch for compatibility
    static constexpr uint64_t NullEpoch = 0;
    static constexpr uint64_t NullSessionID = 0;
    static constexpr uint64_t NullNodeID = 0;
    /// Shard ID starts with 0
    static constexpr uint32_t NullShardID = std::numeric_limits<uint32_t>::max();
    static constexpr uint32_t NullReplicaID = std::numeric_limits<uint32_t>::max();
    static constexpr uint32_t VirtualReplicaID = std::numeric_limits<uint32_t>::max() - 1;
    static constexpr DB::UUID NullNodeUUID = {};
    static constexpr DB::UUID NullStreamID = {};
    static constexpr uint64_t NullCorrelationID = 0;
};
}
