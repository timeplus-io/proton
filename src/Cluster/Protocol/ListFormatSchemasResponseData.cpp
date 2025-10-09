#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ListFormatSchemasResponseData.h>

#include <IO/WriteHelpers.h>

namespace cluster::protocol
{
void ListFormatSchemasResponseData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    err.serialize(wb, version);
    if (!err.hasError())
    {
        cluster::serializeSerializables<true>(descs, wb, version);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void ListFormatSchemasResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    err.deserialize(rb, version);
    if (!err.hasError())
    {
        cluster::deserializeSerializables<true>(descs, rb, version);

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t ListFormatSchemasResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    return err.approximateSerializedSize() + totalApproximateSerializedSizeOf<true>(descs) + sizeof(replica_leader) + sizeof(sn);
}

std::string ListFormatSchemasResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    return fmt::format(
        "{} descs=[{}] replica_leader=0x{:x} sn={}", err.string(), cluster::stringOfStringables<true>(descs), replica_leader, sn);
}
}
