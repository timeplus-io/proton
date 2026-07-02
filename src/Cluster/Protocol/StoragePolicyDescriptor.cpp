#include <Cluster/Protocol/StoragePolicyDescriptor.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <algorithm>

namespace cluster::protocol
{

size_t VolumeDescriptor::approximateSerializedSize() const noexcept
{
    size_t total_size = approximateSerializedSizeOf(name) + sizeof(type) + sizeof(max_data_part_size) + sizeof(max_data_part_size_ratio)
        + sizeof(perform_ttl_move_on_insert) + sizeof(prefer_not_to_merge) + sizeof(load_balancing) + sizeof(volume_priority)
        + sizeof(least_used_ttl_ms) + sizeof(disk_names.size());

    for (const auto & disk_name : disk_names)
        total_size += approximateSerializedSizeOf(disk_name);

    return total_size;
}

void VolumeDescriptor::serialize(DB::WriteBuffer & wb, uint16_t /*version_*/) const
{
    DB::writeStringBinary(name, wb);
    cluster::serializeEnum<VolumeType>(type, wb);
    DB::writeVarUInt(max_data_part_size, wb);
    DB::writeBinary(max_data_part_size_ratio, wb);
    DB::writeBinary(perform_ttl_move_on_insert, wb);
    cluster::serializeEnum<VolumeLoadBalancing>(load_balancing, wb);
    DB::writeVarUInt(volume_priority, wb);
    cluster::serialize(disk_names, wb);
    /// Appended in StoragePolicyDescriptor schema v2.
    DB::writeBinary(prefer_not_to_merge, wb);
    DB::writeVarUInt(least_used_ttl_ms, wb);
}

void VolumeDescriptor::deserialize(DB::ReadBuffer & rb, uint16_t version_)
{
    /// v1 fields — always present.
    DB::readStringBinary(name, rb);
    type = cluster::deserializeEnum<VolumeType>(rb);
    DB::readVarUInt(max_data_part_size, rb);
    DB::readBinary(max_data_part_size_ratio, rb);
    DB::readBinary(perform_ttl_move_on_insert, rb);
    load_balancing = cluster::deserializeEnum<VolumeLoadBalancing>(rb);
    DB::readVarUInt(volume_priority, rb);
    cluster::deserialize(disk_names, rb);

    /// v2 fields — only present when the wire schema_version is v2 or later. On a v1 payload
    /// the new fields stay at their struct defaults (prefer_not_to_merge=false,
    /// least_used_ttl_ms=60000). `version_` is the wire schema_version threaded down from
    /// StoragePolicyDescriptor::deserialize.
    if (version_ >= 2)
    {
        DB::readBinary(prefer_not_to_merge, rb);
        DB::readVarUInt(least_used_ttl_ms, rb);
    }
}

std::string VolumeDescriptor::string() const
{
    return fmt::format("name={}, volumes: [{}]", name, fmt::join(disk_names, ","));
}

size_t StoragePolicyDescriptor::approximateSerializedSize() const noexcept
{
    size_t total_size = sizeof(StoragePolicyDescriptor) + approximateSerializedSizeOf(name, created_by, last_modified_by);

    total_size += totalApproximateSerializedSizeOf<false>(volumes);

    return total_size;
}

void StoragePolicyDescriptor::serialize(DB::WriteBuffer & wb, uint16_t /*version_*/) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeStringBinary(name, wb);
    DB::writeBinary(move_factor, wb);
    cluster::serializeSerializables<false>(volumes, wb, schema_version);

    DB::writeVarInt(create_timestamp_ms, wb);
    DB::writeVarInt(last_modify_timestamp_ms, wb);
    DB::writeStringBinary(created_by, wb);
    DB::writeStringBinary(last_modified_by, wb);
}

void StoragePolicyDescriptor::deserialize(DB::ReadBuffer & rb, uint16_t /*version_*/)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    /// Extract the wire schema_version (high 32 bits) so we can gate the deserialization of
    /// fields added in v2+. The local `version_` parameter is the caller's compile-time
    /// expectation — for backwards compatibility we have to honor what the writer actually
    /// produced, not what this build expects.
    auto wire_schema_version = static_cast<uint16_t>(schema_data_version >> 32);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    DB::readStringBinary(name, rb);
    DB::readBinary(move_factor, rb);
    cluster::deserializeSerializables<false>(volumes, rb, wire_schema_version);

    DB::readVarInt(create_timestamp_ms, rb);
    DB::readVarInt(last_modify_timestamp_ms, rb);
    DB::readStringBinary(created_by, rb);
    DB::readStringBinary(last_modified_by, rb);
}

std::string StoragePolicyDescriptor::string() const
{
    std::vector<std::string> volume_strings;
    std::for_each(volumes.begin(), volumes.end(), [&volume_strings](const auto & v) { volume_strings.push_back(v.string()); });

    return fmt::format(
        "data_version={} name={} volumes=[{}] create_timestamp={} last_modify_timestamp={} created_by={} last_modified_by={}",
        data_version,
        name,
        fmt::join(volume_strings, ";"),
        create_timestamp_ms,
        last_modify_timestamp_ms,
        created_by,
        last_modified_by);
}
}
