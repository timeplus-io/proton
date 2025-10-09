#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ListFormatSchemasRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void ListFormatSchemasRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(schema_name, wb);
    DB::writeStringBinary(format, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void ListFormatSchemasRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(schema_name, rb);
    DB::readStringBinary(format, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t ListFormatSchemasRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(schema_name, format) + sizeof(initiator) + sizeof(consistent_read) + sizeof(timeout_ms);
}

std::string ListFormatSchemasRequestData::doString() const
{
    return fmt::format(
        "schema_name={} format={} initiator=0x{:x} consistent_read={} timeout_ms={}",
        schema_name,
        format,
        initiator,
        consistent_read,
        timeout_ms);
}

}
