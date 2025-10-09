#include <Cluster/Protocol/FormatSchemaDescriptor.h>

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>

#include <fmt/format.h>

namespace cluster::protocol
{

void FormatSchemaDescriptor::serialize(DB::WriteBuffer & wb, uint16_t /*data_version_*/) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeStringBinary(name, wb);
    DB::writeStringBinary(format, wb);
    DB::writeStringBinary(schema_body, wb);

    DB::writeVarInt(create_timestamp_ms, wb);
    DB::writeVarInt(last_modify_timestamp_ms, wb);
    DB::writeStringBinary(created_by, wb);
    DB::writeStringBinary(last_modified_by, wb);
}

void FormatSchemaDescriptor::deserialize(DB::ReadBuffer & rb, uint16_t /*data_version_*/)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);
    auto decoded_schema_version = schema_data_version >> 32;

    DB::readStringBinary(name, rb);
    DB::readStringBinary(format, rb);
    DB::readStringBinary(schema_body, rb);

    /// In version 1, it stored the ExistsOp value in descriptor.
    /// Since it's removed in version 2, just ignore it if it's presented
    if (decoded_schema_version < 2)
    {
        uint8_t ignore_exists_op;
        DB::readIntBinary(ignore_exists_op, rb);
    }

    if (decoded_schema_version >= 2)
    {
        DB::readVarInt(create_timestamp_ms, rb);
        DB::readVarInt(last_modify_timestamp_ms, rb);
        DB::readStringBinary(created_by, rb);
        DB::readStringBinary(last_modified_by, rb);
    }
}

size_t FormatSchemaDescriptor::approximateSerializedSize() const noexcept
{
    return sizeof(schema_version) + sizeof(data_version) + sizeof(create_timestamp_ms) + sizeof(last_modify_timestamp_ms)
        + approximateSerializedSizeOf(name, format, schema_body, created_by, last_modified_by);
}

std::string FormatSchemaDescriptor::string() const
{
    return fmt::format(
        "data_version={} schema_name={} format={} schema_body={} create_timestamp_ms={} last_modify_timestamp_ms={} created_by={} "
        "last_modified_by={}",
        data_version,
        name,
        format,
        schema_body,
        create_timestamp_ms,
        last_modify_timestamp_ms,
        created_by,
        last_modified_by);
}
}
