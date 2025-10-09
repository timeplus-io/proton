#include <Cluster/Protocol/GetStreamRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void GetStreamRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(stream, wb);
    DB::writeVarUInt(versions_requested, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void GetStreamRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(stream, rb);
    DB::readVarUInt(versions_requested, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t GetStreamRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(ns, stream) + sizeof(versions_requested) + sizeof(initiator) + sizeof(consistent_read)
        + sizeof(timeout_ms);
}

std::string GetStreamRequestData::doString() const
{
    return fmt::format(
        "ns={} stream={} versions_requested={} initiator=0x{:x} consistent_read={} timeout_ms={}",
        ns,
        stream,
        versions_requested,
        initiator,
        consistent_read,
        timeout_ms);
}
}
