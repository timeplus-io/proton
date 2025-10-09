#pragma once

#include <cstdint>

namespace cluster
{
struct Constants
{
    static constexpr int64_t LogStartSN = 1; /// Manual bootstrap log cluster, required by Raft
    static constexpr uint64_t LogStartEpoch = 1;
    static constexpr uint64_t SegmentStartID = 1;
    static constexpr int64_t LatestSN = -1;
    static constexpr int64_t EarliestSN = -2;
    static constexpr uint64_t SelfAssignedNodeIDBit = 0x80'00'00'00'00'00'00'00ull;
    static constexpr uint64_t SelfAssignedNodeIDMask = ~SelfAssignedNodeIDBit;
    static constexpr uint64_t LocalFetchSessionIDBit = 0x80'00'00'00'00'00'00'00ull;
    static constexpr uint32_t InternalCheckpointShardOffset = 1'000;
};
}
