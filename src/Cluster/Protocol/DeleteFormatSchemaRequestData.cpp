#include <Cluster/Protocol/DeleteFormatSchemaRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void DeleteFormatSchemaRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(schema_name, wb);
    DB::writeStringBinary(format, wb);
    DB::writeBinary(if_exists, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DeleteFormatSchemaRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(schema_name, rb);
    DB::readStringBinary(format, rb);
    DB::readBinary(if_exists, rb);
    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t DeleteFormatSchemaRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(schema_name) + approximateSerializedSizeOf(format) + sizeof(if_exists) + sizeof(initiator)
        + sizeof(timeout_ms);
}

std::string DeleteFormatSchemaRequestData::doString() const
{
    return fmt::format(
        "schema_name={} format={} if_exists={} initiator=0x{:x} timeout_ms={}", schema_name, format, if_exists, initiator, timeout_ms);
}

}
