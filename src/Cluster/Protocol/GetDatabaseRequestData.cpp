#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/GetDatabaseRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void GetDatabaseRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(name, wb);
    DB::writeVarUInt(versions_requested, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void GetDatabaseRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(name, rb);
    DB::readVarUInt(versions_requested, rb);
    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t GetDatabaseRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(name) + sizeof(versions_requested) + sizeof(initiator) + sizeof(consistent_read)
        + sizeof(timeout_ms);
}

std::string GetDatabaseRequestData::doString() const
{
    return fmt::format(
        "name={} versions_requested={} initiator=0x{:x} consistent_read={} timeout_ms={}",
        name,
        versions_requested,
        initiator,
        consistent_read,
        timeout_ms);
}

}
