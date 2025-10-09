#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/DeleteDatabaseRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>


namespace cluster::protocol
{
void DeleteDatabaseRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(name, wb);
    DB::writeStringBinary(ddl_query, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DeleteDatabaseRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(name, rb);
    DB::readStringBinary(ddl_query, rb);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t DeleteDatabaseRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(ddl_query) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string DeleteDatabaseRequestData::doString() const
{
    return fmt::format("database={} initiator=0x{:x} timeout_ms={}", name, initiator, timeout_ms);
}

}
