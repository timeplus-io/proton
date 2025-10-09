#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ListStoragePoliciesRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void ListStoragePoliciesRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(name, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void ListStoragePoliciesRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(name, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t ListStoragePoliciesRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(name) + sizeof(initiator) + sizeof(consistent_read) + sizeof(timeout_ms);
}

std::string ListStoragePoliciesRequestData::doString() const
{
    return fmt::format("storage policy={} initiator=0x{:x} consistent_read={} timeout_ms={}", name, initiator, consistent_read, timeout_ms);
}

}
