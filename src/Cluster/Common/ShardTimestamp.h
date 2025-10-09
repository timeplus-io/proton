#pragma once

#include <Cluster/Common/Nulls.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster
{
struct ShardTimestamp
{
    ShardTimestamp() = default;

    ShardTimestamp(uint32_t shard_, int64_t timestamp_) : shard(shard_), timestamp(timestamp_) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version);
    size_t approximateSerializedSize() const noexcept { return sizeof(ShardTimestamp); }
    std::string string() const;

    uint32_t shard = Nulls::NullShardID;
    int64_t timestamp = 0;
};

using ShardTimestamps = std::vector<ShardTimestamp>;
}
