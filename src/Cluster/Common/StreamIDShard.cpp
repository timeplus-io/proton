#include <Cluster/Common/StreamIDShard.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void StreamIDShard::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeBinary(id, wb);
    DB::writeVarUInt(shard, wb);
}

void StreamIDShard::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readBinary(id, rb);
    DB::readVarUInt(shard, rb);
}

size_t StreamIDShard::approximateSerializedSize() const noexcept
{
    return sizeof(StreamIDShard);
}

std::string StreamIDShard::string() const
{
    return fmt::format("id={} shard={}", DB::toString(id), shard);
}

std::string StreamIDShard::idString() const
{
    return DB::toString(id);
}

}
