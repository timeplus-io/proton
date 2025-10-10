#include <Cluster/Protocol/NamedCollectionDescriptor.h>

#include <Cluster/Common/serde.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <cstddef>

namespace cluster::protocol
{

void NamedCollectionDescriptor::serialize(DB::WriteBuffer & wb, [[maybe_unused]] uint16_t version_) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeVarUInt(entries.size(), wb);
    for (const auto & entry : entries)
    {
        DB::writeStringBinary(entry.key, wb);
        DB::writeStringBinary(entry.value, wb);
        cluster::serializeEnum<NamedCollectionOverridability>(entry.overridability, wb);
    }

    DB::writeVarInt(create_timestamp_ms, wb);
    DB::writeVarInt(last_modify_timestamp_ms, wb);
    DB::writeStringBinary(created_by, wb);
    DB::writeStringBinary(last_modified_by, wb);
}

void NamedCollectionDescriptor::deserialize(DB::ReadBuffer & rb, [[maybe_unused]] uint16_t version_)
{
    uint64_t schema_data_version = 0;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    size_t entries_size = 0;
    DB::readVarUInt(entries_size, rb);
    entries.reserve(entries_size);
    for (size_t i = 0; i < entries_size; ++i)
    {
        NamedCollectionEntry entry;
        DB::readStringBinary(entry.key, rb);
        DB::readStringBinary(entry.value, rb);
        entry.overridability = cluster::deserializeEnum<NamedCollectionOverridability>(rb);
        entries.emplace_back(std::move(entry));
    }

    DB::readVarInt(create_timestamp_ms, rb);
    DB::readVarInt(last_modify_timestamp_ms, rb);
    DB::readStringBinary(created_by, rb);
    DB::readStringBinary(last_modified_by, rb);
}

[[nodiscard]] size_t NamedCollectionDescriptor::approximateSerializedSize() const noexcept
{
    size_t total_size = sizeof(uint64_t) + sizeof(create_timestamp_ms) + sizeof(last_modify_timestamp_ms)
        + approximateSerializedSizeOf(created_by, last_modified_by);

    total_size += sizeof(size_t);
    for (const auto & entry : entries)
        total_size += approximateSerializedSizeOf(entry.key, entry.value) + sizeof(entry.overridability);

    return total_size;
}

[[nodiscard]] std::string NamedCollectionDescriptor::string() const
{
    return fmt::format(
        "data_version={} entries={} create_timestamp_ms={} last_modify_timestamp_ms={} created_by={} last_modified_by={}",
        data_version,
        entries.size(),
        create_timestamp_ms,
        last_modify_timestamp_ms,
        created_by,
        last_modified_by);
}

}
