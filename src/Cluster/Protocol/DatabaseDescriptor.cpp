#include <Cluster/Protocol/DatabaseDescriptor.h>

#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>

#include <fmt/format.h>


namespace cluster::protocol
{

void DatabaseDescriptor::serialize(DB::WriteBuffer & wb, uint16_t /*data_version_*/) const
{
    DB::writeVarUInt(schema_version, wb);
    DB::writeStringBinary(name, wb);
    DB::writeStringBinary(sql_def, wb);
    DB::writeStringBinary(comment, wb);
    DB::writeBinary(uuid, wb);

    DB::writeVarInt(create_timestamp_ms, wb);
    DB::writeVarInt(last_modify_timestamp_ms, wb);
    DB::writeStringBinary(created_by, wb);
    DB::writeStringBinary(last_modified_by, wb);
}

void DatabaseDescriptor::deserialize(DB::ReadBuffer & rb, uint16_t /*data_version_*/)
{
    uint32_t schema_version_;
    DB::readVarUInt(schema_version_, rb);
    DB::readStringBinary(name, rb);
    DB::readStringBinary(sql_def, rb);
    DB::readStringBinary(comment, rb);
    DB::readBinary(uuid, rb);

    DB::readVarInt(create_timestamp_ms, rb);
    DB::readVarInt(last_modify_timestamp_ms, rb);
    DB::readStringBinary(created_by, rb);
    DB::readStringBinary(last_modified_by, rb);
}

size_t DatabaseDescriptor::approximateSerializedSize() const noexcept
{
    return sizeof(schema_version) + sizeof(DatabaseDescriptor)
        + approximateSerializedSizeOf(name, sql_def, comment, created_by, last_modified_by);
}

std::string DatabaseDescriptor::string() const
{
    return fmt::format("name={} sql_def={}", name, sql_def);
}
}
