#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/DeleteDiskRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void DeleteDiskRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(name, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DeleteDiskRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(name, rb);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t DeleteDiskRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(name) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string DeleteDiskRequestData::doString() const
{
    return fmt::format("disk={} initiator=0x{:x} timeout_ms={}", name, initiator, timeout_ms);
}

}
