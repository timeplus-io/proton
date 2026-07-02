#include <Cluster/Protocol/ListStoragePoliciesResponseData.h>

#include <IO/WriteHelpers.h>
#include <fmt/ranges.h>

namespace cluster::protocol
{

void ListStoragePoliciesResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);
    if (!err.hasError())
    {
        DB::writeVarUInt(policies.size(), wb);
        for (const auto & desc : policies)
            desc->serialize(wb, Nulls::NullVersion);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void ListStoragePoliciesResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);
    if (!err.hasError())
    {
        uint64_t size = 0;
        DB::readVarUInt(size, rb);
        for (size_t i = 0; i < size; ++i)
        {
            policies.emplace_back();
            policies.back()->deserialize(rb, Nulls::NullVersion);
        }

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t ListStoragePoliciesResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    return totalApproximateSerializedSizeOf<true>(policies) + sizeof(replica_leader) + sizeof(sn);
}

std::string ListStoragePoliciesResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    std::vector<std::string> strs;
    strs.reserve(policies.size());

    for (const auto & desc : policies)
        strs.emplace_back(desc->string());

    return fmt::format("policies=[{}] leader_replica=0x{:x} sn={}", fmt::join(strs, ";"), replica_leader, sn);
}

}
