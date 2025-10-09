#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/GetFormatSchemaRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void GetFormatSchemaRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(schema_name, wb);
    DB::writeStringBinary(format, wb);
    DB::writeVarUInt(versions_requested, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void GetFormatSchemaRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(schema_name, rb);
    DB::readStringBinary(format, rb);
    DB::readVarUInt(versions_requested, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t GetFormatSchemaRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(schema_name, format) + sizeof(versions_requested) + sizeof(initiator) + sizeof(consistent_read)
        + sizeof(timeout_ms);
}

std::string GetFormatSchemaRequestData::doString() const
{
    return fmt::format(
        "schema_name={} format={} versions_requested={} initiator=0x{:x} consistent_read={} timeout_ms={}",
        schema_name,
        format,
        versions_requested,
        initiator,
        consistent_read,
        timeout_ms);
}

}
