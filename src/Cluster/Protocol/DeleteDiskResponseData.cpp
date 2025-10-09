#include <Cluster/Protocol/DeleteDiskResponseData.h>

#include <IO/WriteHelpers.h>

namespace cluster::protocol
{
void DeleteDiskResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);

    DB::writeVarUInt(replica_leader, wb);
    DB::writeVarInt(sn, wb);
}

void DeleteDiskResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);

    DB::readVarUInt(replica_leader, rb);
    DB::readVarInt(sn, rb);
}

size_t DeleteDiskResponseData::approximateSerializedSize() const noexcept
{
    return err.approximateSerializedSize() + sizeof(replica_leader) + sizeof(sn);
}

std::string DeleteDiskResponseData::doString() const
{
    return fmt::format("{} replica_leader=0x{:x} sn={}", err.string(), replica_leader, sn);
}

}
