#include <Cluster/Protocol/DiskDescriptor.h>

#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>


namespace cluster::protocol
{
size_t DiskDescriptor::approximateSerializedSize() const noexcept
{
    return sizeof(DiskDescriptor) + approximateSerializedSizeOf(name, config, created_by, last_modified_by);
}

void DiskDescriptor::serialize(DB::WriteBuffer & wb, uint16_t /*version_*/) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    cluster::serializeEnum<DiskType>(type, wb);
    DB::writeStringBinary(name, wb);
    DB::writeStringBinary(config, wb);

    DB::writeVarInt(create_timestamp_ms, wb);
    DB::writeVarInt(last_modify_timestamp_ms, wb);
    DB::writeStringBinary(created_by, wb);
    DB::writeStringBinary(last_modified_by, wb);
}

void DiskDescriptor::deserialize(DB::ReadBuffer & rb, uint16_t /*version_*/)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    type = cluster::deserializeEnum<DiskType>(rb);
    DB::readStringBinary(name, rb);
    DB::readStringBinary(config, rb);

    DB::readVarInt(create_timestamp_ms, rb);
    DB::readVarInt(last_modify_timestamp_ms, rb);
    DB::readStringBinary(created_by, rb);
    DB::readStringBinary(last_modified_by, rb);
}

std::string DiskDescriptor::string() const
{
    return fmt::format(
        "data_version={} name={} type={} config=[{}] create_timestamp={} last_modify_timestamp={} created_by={} last_modified_by={}",
        data_version,
        name,
        type,
        config,
        create_timestamp_ms,
        last_modify_timestamp_ms,
        created_by,
        last_modified_by);
}
}
