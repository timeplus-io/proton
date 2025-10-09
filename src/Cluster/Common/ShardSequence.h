#pragma once

#include <Cluster/Common/Nulls.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster
{
struct ShardSequence
{
    ShardSequence() = default;

    ShardSequence(uint32_t shard_, int64_t sn_) : shard(shard_), sn(sn_) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version);
    size_t approximateSerializedSize() const noexcept { return sizeof(ShardSequence); }
    std::string string() const;

    uint32_t shard = Nulls::NullShardID;
    int64_t sn = 0;
};

using ShardSequences = std::vector<ShardSequence>;
}
