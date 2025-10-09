#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/ListDisksResponseData.h>

#include <IO/WriteHelpers.h>

#include <ranges>

namespace cluster::protocol
{
void ListDisksResponseData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    err.serialize(wb, Nulls::NullVersion);
    if (!err.hasError())
    {
        cluster::serializeSerializables<true>(disks, wb, version);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void ListDisksResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    err.deserialize(rb, Nulls::NullVersion);
    if (!err.hasError())
    {
        cluster::deserializeSerializables<true>(disks, rb, version);

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t ListDisksResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    return totalApproximateSerializedSizeOf<true>(disks) + sizeof(replica_leader) + sizeof(sn);
}

std::string ListDisksResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    auto disk_view = disks | std::views::transform([](const auto & disk) { return disk->string(); });
    return fmt::format("disks=[{}] replica_leader=0x{:x} sn={}", fmt::join(disk_view, ";"), replica_leader, sn);
}

}
