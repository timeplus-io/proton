#include <Cluster/Protocol/PipPythonPackageRequestData.h>

#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{

PipPythonPackageRequestData::~PipPythonPackageRequestData() = default;

void PipPythonPackageRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    cluster::serializeEnum(operation, wb);

    DB::writeVarUInt(package_names.size(), wb);
    for (const auto & name : package_names)
        DB::writeStringBinary(name, wb);

    DB::writeVarUInt(package_versions.size(), wb);
    for (const auto & version : package_versions)
        DB::writeStringBinary(version, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void PipPythonPackageRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    operation = cluster::deserializeEnum<Operation>(rb);

    size_t names_count;
    DB::readVarUInt(names_count, rb);
    package_names.resize(names_count);
    for (size_t i = 0; i < names_count; ++i)
        DB::readStringBinary(package_names[i], rb);

    size_t versions_count;
    DB::readVarUInt(versions_count, rb);
    package_versions.resize(versions_count);
    for (size_t i = 0; i < versions_count; ++i)
        DB::readStringBinary(package_versions[i], rb);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t PipPythonPackageRequestData::approximateSerializedSize() const noexcept
{
    size_t size = sizeof(operation) + sizeof(initiator) + sizeof(timeout_ms);
    /// package_names size
    size += sizeof(size_t);
    for (const auto & name : package_names)
        size += approximateSerializedSizeOf(name);

    /// package_versions size
    size += sizeof(size_t);
    for (const auto & version : package_versions)
        size += approximateSerializedSizeOf(version);
    return size;
}

std::string PipPythonPackageRequestData::doString() const
{
    std::string packages_str;
    for (size_t i = 0; i < package_names.size(); ++i)
    {
        if (i > 0)
            packages_str += ", ";
        packages_str += package_names[i];
        if (i < package_versions.size() && !package_versions[i].empty())
            packages_str += "==" + package_versions[i];
    }

    return fmt::format(
        "operation={} packages=[{}] initiator=0x{:x} timeout_ms={}", magic_enum::enum_name(operation), packages_str, initiator, timeout_ms);
}
}
