#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/ListUserDefinedFunctionsResponseData.h>

#include <IO/WriteHelpers.h>

#include <ranges>

namespace cluster::protocol
{
void ListUserDefinedFunctionsResponseData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    err.serialize(wb, Nulls::NullVersion);
    if (!err.hasError())
    {
        cluster::serializeSerializables<true>(descs, wb, version);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void ListUserDefinedFunctionsResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    err.deserialize(rb, Nulls::NullVersion);
    if (!err.hasError())
    {
        cluster::deserializeSerializables<true>(descs, rb, version);

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t ListUserDefinedFunctionsResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    return totalApproximateSerializedSizeOf<true>(descs) + sizeof(replica_leader) + sizeof(sn);
}

std::string ListUserDefinedFunctionsResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    auto udf_view = descs | std::views::transform([](const auto & desc) { return desc->string(); });
    return fmt::format("funcs=[{}] replica_leader=0x{:x} sn={}", fmt::join(udf_view, ";"), replica_leader, sn);
}

}
