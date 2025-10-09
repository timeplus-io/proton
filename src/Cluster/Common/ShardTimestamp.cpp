#include <Cluster/Common/ShardTimestamp.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void ShardTimestamp::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarUInt(shard, wb);
    DB::writeVarInt(timestamp, wb);
}

void ShardTimestamp::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readVarUInt(shard, rb);
    DB::readVarInt(timestamp, rb);
}

std::string ShardTimestamp::string() const
{
    return fmt::format("shard={} timestamp={}", shard, timestamp);
}

}
