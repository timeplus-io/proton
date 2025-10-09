#include <Cluster/Protocol/DeleteAlertRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void DeleteAlertRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(name, wb);
    DB::writeBinary(id, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DeleteAlertRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(name, rb);
    DB::readBinary(id, rb);
    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t DeleteAlertRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(ns) + approximateSerializedSizeOf(name) + sizeof(id) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string DeleteAlertRequestData::doString() const
{
    return fmt::format("ns={} name={} id={} initiator=0x{:x} timeout_ms={}", ns, name, toString(id), initiator, timeout_ms);
}

}
