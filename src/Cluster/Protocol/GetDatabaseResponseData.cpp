#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/GetDatabaseResponseData.h>

#include <IO/WriteHelpers.h>

#include <ranges>

namespace cluster::protocol
{
void GetDatabaseResponseData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    err.serialize(wb, Nulls::NullVersion);
    if (!err.hasError())
    {
        cluster::serializeSerializables<true>(databases, wb, version);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void GetDatabaseResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    err.deserialize(rb, Nulls::NullVersion);
    if (!err.hasError())
    {
        cluster::deserializeSerializables<true>(databases, rb, version);

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t GetDatabaseResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    return totalApproximateSerializedSizeOf<true>(databases) + sizeof(replica_leader) + sizeof(sn);
}

std::string GetDatabaseResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    auto db_view = databases | std::views::transform([](const auto & desc) { return desc->string(); });
    return fmt::format("databases=[{}] replica_leader=0x{:x} sn={}", fmt::join(db_view, ";"), replica_leader, sn);
}

}
