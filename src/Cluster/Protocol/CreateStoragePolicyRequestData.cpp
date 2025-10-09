#include <Cluster/Protocol/CreateStoragePolicyRequestData.h>

#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{

void CreateStoragePolicyRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    chassert(policy);
    policy->serialize(wb, /*version=*/1);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void CreateStoragePolicyRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    policy = std::make_shared<StoragePolicyDescriptor>();
    policy->deserialize(rb, /*version=*/1);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t CreateStoragePolicyRequestData::approximateSerializedSize() const noexcept
{
    chassert(policy);
    return policy->approximateSerializedSize() + sizeof(initiator) + sizeof(timeout_ms);
}


std::string CreateStoragePolicyRequestData::doString() const
{
    return fmt::format("storage={{{}}} initiator=0x{:x} tiemout_ms={}", policy ? policy->string() : "null", initiator, timeout_ms);
}

}
