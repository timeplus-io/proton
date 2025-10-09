#include <Cluster/Protocol/DeleteFormatSchemaResponseData.h>

#include <IO/WriteHelpers.h>

namespace cluster::protocol
{
void DeleteFormatSchemaResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);

    DB::writeVarUInt(replica_leader, wb);
    DB::writeVarInt(sn, wb);
}

void DeleteFormatSchemaResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);

    DB::readVarUInt(replica_leader, rb);
    DB::readVarInt(sn, rb);
}

size_t DeleteFormatSchemaResponseData::approximateSerializedSize() const noexcept
{
    return err.approximateSerializedSize() + sizeof(replica_leader) + sizeof(sn);
}

}
