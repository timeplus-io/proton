#include <Cluster/Protocol/ListAccessEntitiesRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void ListAccessEntitiesRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void ListAccessEntitiesRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t ListAccessEntitiesRequestData::approximateSerializedSize() const noexcept
{
    return sizeof(initiator) + sizeof(consistent_read) + sizeof(timeout_ms);
}

std::string ListAccessEntitiesRequestData::doString() const
{
    return fmt::format("initiator=0x{:x} consistent_read={} timeout_ms={}", initiator, consistent_read, timeout_ms);
}

}
