#include <Cluster/Common/ShardSequence.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void ShardSequence::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarUInt(shard, wb);
    DB::writeVarInt(sn, wb);
}

void ShardSequence::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readVarUInt(shard, rb);
    DB::readVarInt(sn, rb);
}

std::string ShardSequence::string() const
{
    return fmt::format("shard={} sn={}", shard, sn);
}

}
