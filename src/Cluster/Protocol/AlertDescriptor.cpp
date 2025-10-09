#include <Cluster/Protocol/AlertDescriptor.h>

#include <Cluster/Common/serde.h>
#include <IO/VarInt.h>

namespace cluster::protocol
{
void AlertDescriptor::PeriodicCount::serialize(DB::WriteBuffer & wb, uint16_t /*version_*/) const
{
    DB::writeVarUInt(count, wb);
    DB::writeVarUInt(interval, wb);
    cluster::serializeEnum(interval_kind.kind, wb);
}

void AlertDescriptor::PeriodicCount::deserialize(DB::ReadBuffer & rb, uint16_t /*version_*/)
{
    DB::readVarUInt(count, rb);
    DB::readVarUInt(interval, rb);
    interval_kind.kind = cluster::deserializeEnum<DB::IntervalKind::Kind>(rb);
}

size_t AlertDescriptor::PeriodicCount::approximateSerializedSize() const noexcept
{
    return sizeof(count) + sizeof(interval) + sizeof(std::to_underlying(interval_kind.kind));
}

bool AlertDescriptor::PeriodicCount::operator==(const AlertDescriptor::PeriodicCount & rhs) const
{
    return count == rhs.count && interval == rhs.interval && interval_kind == rhs.interval_kind;
}

std::string AlertDescriptor::PeriodicCount::string() const
{
    return fmt::format("count={} interval={} interval_kind={}", count, interval, interval_kind.toString());
}

void AlertDescriptor::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeBinary(id, wb);
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(name, wb);
    DB::writeStringBinary(function_name, wb);
    DB::writeStringBinary(select_query, wb);
    batch.serialize(wb, version);
    limit.serialize(wb, version);

    DB::writeVarInt(create_timestamp_ms, wb);
    DB::writeVarInt(last_modify_timestamp_ms, wb);
    DB::writeStringBinary(created_by, wb);
    DB::writeStringBinary(last_modified_by, wb);
}

void AlertDescriptor::deserialize(DB::ReadBuffer & rb, uint16_t version)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);
    /// Once we update schema version to 2+, will need to do version check
    /// auto decoded_schema_version = schema_data_version >> 32;
    DB::readBinary(id, rb);
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(name, rb);
    DB::readStringBinary(function_name, rb);
    DB::readStringBinary(select_query, rb);
    batch.deserialize(rb, version);
    limit.deserialize(rb, version);

    DB::readVarInt(create_timestamp_ms, rb);
    DB::readVarInt(last_modify_timestamp_ms, rb);
    DB::readStringBinary(created_by, rb);
    DB::readStringBinary(last_modified_by, rb);
}

bool AlertDescriptor::operator==(const AlertDescriptor & rhs) const
{
    return data_version == rhs.data_version && ns == rhs.ns && name == rhs.name && id == rhs.id && function_name == rhs.function_name
        && select_query == rhs.select_query && limit == rhs.limit && batch == rhs.batch;
}

size_t AlertDescriptor::approximateSerializedSize() const noexcept
{
    return sizeof(schema_version) + sizeof(data_version) + sizeof(create_timestamp_ms) + sizeof(last_modify_timestamp_ms)
        + batch.approximateSerializedSize() + limit.approximateSerializedSize() + sizeof(id)
        + approximateSerializedSizeOf(ns, name, function_name, select_query, created_by, last_modified_by);
}

std::string AlertDescriptor::string() const
{
    return fmt::format(
        "data_version={} ns={} name={} id={} select_query={} trigger_function={} batch={{}} limit={{}} create_timestamp_ms={} "
        "last_modify_timestamp_ms={} created_by={} last_modified_by={}",
        data_version,
        ns,
        name,
        toString(id),
        select_query,
        function_name,
        batch.string(),
        limit.string(),
        create_timestamp_ms,
        last_modify_timestamp_ms,
        created_by,
        last_modified_by);
}

bool AlertDescriptor::isValid() const noexcept
{
    return data_version > 0 && !ns.empty() && !name.empty() && id != DB::UUIDHelpers::Nil && !function_name.empty()
        && !select_query.empty();
}
}
