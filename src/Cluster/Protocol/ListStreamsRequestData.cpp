#include <Cluster/Protocol/ListStreamsRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void ListStreamsRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(stream, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void ListStreamsRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(stream, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t ListStreamsRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(ns, stream) + sizeof(initiator) + sizeof(consistent_read) + sizeof(timeout_ms);
}

std::string ListStreamsRequestData::doString() const
{
    return fmt::format(
        "ns={} stream={} initiator=0x{:x} consistent_read={} timeout_ms={}", ns, stream, initiator, consistent_read, timeout_ms);
}
}
