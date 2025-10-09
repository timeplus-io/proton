#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/GetStreamResponseData.h>

#include <IO/WriteHelpers.h>

#include <ranges>

namespace cluster::protocol
{
void GetStreamResponseData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    err.serialize(wb, Nulls::NullVersion);
    if (!err.hasError())
    {
        cluster::serializeSerializables<true>(stream_descs, wb, version);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void GetStreamResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    err.deserialize(rb, Nulls::NullVersion);
    if (!err.hasError())
    {
        cluster::deserializeSerializables<true>(stream_descs, rb, version);

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t GetStreamResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    return err.approximateSerializedSize() + cluster::approximateSerializedSizeOf(stream_descs) + sizeof(replica_leader) + sizeof(sn);
}

std::string GetStreamResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    auto stream_view = stream_descs | std::views::transform([](const auto & desc) { return desc->string(); });
    return fmt::format("[{}], replica_leader=0x{:x} sn={}", fmt::join(stream_view, ";"), replica_leader, sn);
}

}
