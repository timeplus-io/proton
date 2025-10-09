#include <Cluster/Protocol/PipPythonPackageResponseData.h>

#include <Cluster/Common/utils.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void PipPythonPackageResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);
    if (!err.hasError())
    {
        DB::writeVarUInt(package_names.size(), wb);
        for (const auto & name : package_names)
            DB::writeStringBinary(name, wb);

        DB::writeVarUInt(package_versions.size(), wb);
        for (const auto & version : package_versions)
            DB::writeStringBinary(version, wb);

        DB::writeVarUInt(replica_leader, wb);
        DB::writeVarInt(sn, wb);
    }
}

void PipPythonPackageResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);
    if (!err.hasError())
    {
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

        DB::readVarUInt(replica_leader, rb);
        DB::readVarInt(sn, rb);
    }
}

size_t PipPythonPackageResponseData::approximateSerializedSize() const noexcept
{
    if (err.hasError())
        return err.approximateSerializedSize();

    size_t size = err.approximateSerializedSize() + sizeof(replica_leader) + sizeof(sn);

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

std::string PipPythonPackageResponseData::doString() const
{
    if (err.hasError())
        return err.string();

    std::string packages_str;
    for (size_t i = 0; i < package_names.size(); ++i)
    {
        if (i > 0)
            packages_str += ", ";
        packages_str += package_names[i];
        if (i < package_versions.size() && !package_versions[i].empty())
            packages_str += "==" + package_versions[i];
    }

    return fmt::format("packages=[{}] replica_leader=0x{:x} sn={}", packages_str, replica_leader, sn);
}

}
