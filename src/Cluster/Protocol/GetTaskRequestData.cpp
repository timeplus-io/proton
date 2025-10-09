#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/GetTaskRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>


namespace cluster::protocol
{
void GetTaskRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(name, wb);
    DB::writeVarUInt(versions_requested, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void GetTaskRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(name, rb);
    DB::readVarUInt(versions_requested, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t GetTaskRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(ns, name) + sizeof(versions_requested) + sizeof(initiator) + sizeof(consistent_read)
        + sizeof(timeout_ms);
}

std::string GetTaskRequestData::doString() const
{
    return fmt::format(
        "ns={} name={} versions_requested={} initiator=0x{:x} consistent_read={} timeout_ms={}",
        ns,
        name,
        versions_requested,
        initiator,
        consistent_read,
        timeout_ms);
}

}

