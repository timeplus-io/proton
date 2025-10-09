#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/StreamShard.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void StreamShard::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(name, wb);
    id_shard.serialize(wb, Nulls::NullVersion);
}

void StreamShard::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(name, rb);
    id_shard.deserialize(rb, Nulls::NullVersion);
}

std::string StreamShard::string() const
{
    return fmt::format("ns={} name={} id={} shard={}", ns, name, DB::toString(id_shard.id), id_shard.shard);
}

}
