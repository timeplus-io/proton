#include <Cluster/Protocol/CreateAlertResponseData.h>

#include <IO/WriteHelpers.h>

namespace cluster::protocol
{
void CreateAlertResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);
    if (!err.hasError())
    {
        desc.serialize(wb, Nulls::NullVersion);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void CreateAlertResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);
    if (!err.hasError())
    {
        desc.deserialize(rb, Nulls::NullVersion);

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t CreateAlertResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    return err.approximateSerializedSize() + desc.approximateSerializedSize() + sizeof(replica_leader) + sizeof(sn);
}

std::string CreateAlertResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    return fmt::format("{} replica_leader=0x{:x} sn={}", desc.string(), replica_leader, sn);
}

}
