#include <Cluster/Protocol/CreateTaskResponseData.h>

#include <IO/WriteHelpers.h>


namespace cluster::protocol
{
void CreateTaskResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);
    if (!err.hasError())
    {
        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void CreateTaskResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);
    if (!err.hasError())
    {
        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t CreateTaskResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    return err.approximateSerializedSize() + sizeof(replica_leader) + sizeof(sn);
}

std::string CreateTaskResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    return fmt::format("replica_leader=0x{:x} sn={}", replica_leader, sn);
}
}
