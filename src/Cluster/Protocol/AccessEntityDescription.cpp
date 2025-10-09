#include <Cluster/Protocol/AccessEntityDescription.h>

#include <Cluster/Common/utils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace cluster::protocol
{

void AccessEntityDescription::serialize(DB::WriteBuffer & wb, uint16_t /*version_*/) const
{
    DB::writeBinary(id, wb);

    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeStringBinary(entity, wb);
}

void AccessEntityDescription::deserialize(DB::ReadBuffer & rb, uint16_t /*version_*/)
{
    DB::readBinary(id, rb);

    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    DB::readStringBinary(entity, rb);
}

size_t AccessEntityDescription::approximateSerializedSize() const noexcept
{
    return sizeof(id) + sizeof(schema_version) + sizeof(data_version) + approximateSerializedSizeOf(entity);
}

std::string AccessEntityDescription::string() const
{
    return fmt::format("id={} version={}", DB::toString(id), data_version);
}

}
