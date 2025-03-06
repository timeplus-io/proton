#pragma once

#include <cstdint>

namespace DB
{

namespace Kafka
{

struct WatermarkOffsets
{
    int64_t low{-1};
    int64_t high{-1};
};

struct PartitionTimestamp
{
    PartitionTimestamp(int32_t partition_, int64_t timestamp_) : partition(partition_), timestamp(timestamp_) { }

    int32_t partition;
    int64_t timestamp;
};

}

}
