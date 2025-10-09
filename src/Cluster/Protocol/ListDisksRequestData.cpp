#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ListDisksRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void ListDisksRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(name, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void ListDisksRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(name, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t ListDisksRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(name) + sizeof(initiator) + sizeof(consistent_read) + sizeof(timeout_ms);
}

std::string ListDisksRequestData::doString() const
{
    return fmt::format("disk={} initiator=0x{:x} consistent_read={} timeout_ms={}", name, initiator, consistent_read, timeout_ms);
}

}
