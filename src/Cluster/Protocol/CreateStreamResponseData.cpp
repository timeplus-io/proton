#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/CreateStreamResponseData.h>

#include <IO/WriteHelpers.h>

namespace cluster::protocol
{
void CreateStreamResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);
    if (!err.hasError())
    {
        stream_desc.serialize(wb, Nulls::NullVersion);
        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void CreateStreamResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);
    if (!err.hasError())
    {
        stream_desc.deserialize(rb, Nulls::NullVersion);
        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t CreateStreamResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    return err.approximateSerializedSize() + stream_desc.approximateSerializedSize() + sizeof(replica_leader) + sizeof(sn);
}

std::string CreateStreamResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    return fmt::format("{} replica_leader=0x{:x} sn={}", stream_desc.string(), replica_leader, sn);
}
}
