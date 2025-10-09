#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/StreamDescriptor.h>

#include <fmt/format.h>

namespace DB::ErrorCodes
{
extern const int OK;
}

namespace cluster::protocol
{

void StreamDescriptor::serialize(DB::WriteBuffer & wb, uint16_t /*data_version_*/) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeVarUInt(flags, wb);

    stream.serialize(wb, Nulls::NullVersion);

    DB::writeVarUInt(shards, wb);

    DB::writeBinary(inmemory, wb);
    cluster::serializeEnum(codec, wb);

    DB::writeVarUInt(flush_ms, wb);
    DB::writeVarUInt(flush_messages, wb);
    DB::writeVarUInt(flush_bytes, wb);
    DB::writeVarUInt(retention_ms, wb);
    DB::writeVarUInt(retention_bytes, wb);

    DB::writeVarInt(create_timestamp_ms, wb);
    DB::writeVarInt(last_modify_timestamp_ms, wb);

    DB::writeStringBinary(created_by, wb);
    DB::writeStringBinary(last_modified_by, wb);
    DB::writeStringBinary(sql_def, wb);
}

void StreamDescriptor::deserialize(DB::ReadBuffer & rb, uint16_t /*data_version_*/)
{
    uint64_t schema_data_version = 0;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    DB::readVarUInt(flags, rb);

    stream.deserialize(rb, Nulls::NullVersion);

    DB::readVarUInt(shards, rb);

    DB::readBinary(inmemory, rb);

    codec = cluster::deserializeEnum<DB::CompressionMethodByte>(rb);

    DB::readVarUInt(flush_ms, rb);
    DB::readVarUInt(flush_messages, rb);
    DB::readVarUInt(flush_bytes, rb);
    DB::readVarUInt(retention_ms, rb);
    DB::readVarUInt(retention_bytes, rb);

    DB::readVarInt(create_timestamp_ms, rb);
    DB::readVarInt(last_modify_timestamp_ms, rb);

    DB::readStringBinary(created_by, rb);
    DB::readStringBinary(last_modified_by, rb);
    DB::readStringBinary(sql_def, rb);
}

size_t StreamDescriptor::approximateSerializedSize() const noexcept
{
    return sizeof(StreamDescriptor) + stream.approximateSerializedSize()
        + approximateSerializedSizeOf(created_by, last_modified_by, sql_def);
}

bool StreamDescriptor::operator==(const StreamDescriptor & rhs) const
{
    return data_version == rhs.data_version && flags == rhs.flags && stream == rhs.stream && shards == rhs.shards && codec == rhs.codec
        && flush_ms == rhs.flush_ms && flush_messages == rhs.flush_messages && flush_bytes == rhs.flush_bytes
        && retention_ms == rhs.retention_ms && retention_bytes == rhs.retention_bytes && create_timestamp_ms == rhs.create_timestamp_ms
        && last_modify_timestamp_ms == rhs.last_modify_timestamp_ms && sql_def == rhs.sql_def;
}


std::string StreamDescriptor::string() const
{
    return fmt::format(
        "data_version={} flags=0x{:x} {} shards={} inmemory={} codec={} flush_ms={} "
        "flush_messages={} flush_bytes={} retention_ms={} retention_bytes={} create_timestamp={} last_modify_timestamp={} created_by={} "
        "last_modified_by={} sql_def={}",
        data_version,
        flags,
        stream.string(),
        shards,
        inmemory,
        magic_enum::enum_name(codec),
        flush_ms,
        flush_messages,
        flush_bytes,
        retention_ms,
        retention_bytes,
        create_timestamp_ms,
        last_modify_timestamp_ms,
        created_by,
        last_modified_by,
        sql_def);
}
}
