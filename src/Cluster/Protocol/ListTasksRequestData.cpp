#include <Cluster/Protocol/ListTasksRequestData.h>

#include <Cluster/Common/utils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void ListTasksRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(name, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void ListTasksRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(name, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t ListTasksRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(ns, name) + sizeof(initiator) + sizeof(consistent_read) + sizeof(timeout_ms);
}

std::string ListTasksRequestData::doString() const
{
    return fmt::format("ns={} name={} initiator=0x{:x} consistent_read={} timeout_ms={}", ns, name, initiator, consistent_read, timeout_ms);
}

}
