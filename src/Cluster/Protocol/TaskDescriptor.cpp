#include <Cluster/Protocol/TaskDescriptor.h>

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>

#include <fmt/format.h>


namespace cluster::protocol
{

void TaskDescriptor::serialize(DB::WriteBuffer & wb, uint16_t /*data_version_*/) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeBinary(id, wb);
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(name, wb);

    DB::writeVarInt(create_timestamp_ms, wb);
    DB::writeVarInt(last_modify_timestamp_ms, wb);
    DB::writeStringBinary(created_by, wb);
    DB::writeStringBinary(last_modified_by, wb);

    DB::writeStringBinary(cron, wb);
    DB::writeVarUInt(interval, wb);
    cluster::serializeEnum(interval_unit.kind, wb);

    DB::writeVarUInt(timeout, wb);
    cluster::serializeEnum(timeout_unit.kind, wb);

    DB::writeVarUInt(checkpoint_columns.size(), wb);
    for (const auto & col_name : checkpoint_columns)
        DB::writeStringBinary(col_name, wb);

    DB::writeStringBinary(checkpoint_init_values, wb);

    DB::writeStringBinary(target_database_name, wb);
    DB::writeStringBinary(target_table_name, wb);

    DB::writeStringBinary(sql, wb);

    DB::writeVarUInt(status, wb);
}

void TaskDescriptor::deserialize(DB::ReadBuffer & rb, uint16_t /*data_version_*/)
{
    uint64_t schema_data_version = 0;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    DB::readBinary(id, rb);
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(name, rb);

    DB::readVarInt(create_timestamp_ms, rb);
    DB::readVarInt(last_modify_timestamp_ms, rb);
    DB::readStringBinary(created_by, rb);
    DB::readStringBinary(last_modified_by, rb);

    DB::readStringBinary(cron, rb);
    DB::readVarUInt(interval, rb);
    interval_unit.kind = cluster::deserializeEnum<DB::IntervalKind::Kind>(rb);

    DB::readVarUInt(timeout, rb);
    timeout_unit.kind = cluster::deserializeEnum<DB::IntervalKind::Kind>(rb);

    size_t checkpoint_columns_size;
    DB::readVarUInt(checkpoint_columns_size, rb);
    checkpoint_columns.resize(checkpoint_columns_size);
    for (auto & col_name : checkpoint_columns)
        DB::readStringBinary(col_name, rb);

    DB::readStringBinary(checkpoint_init_values, rb);

    DB::readStringBinary(target_database_name, rb);
    DB::readStringBinary(target_table_name, rb);

    DB::readStringBinary(sql, rb);

    DB::readVarUInt(status, rb);
}

size_t TaskDescriptor::approximateSerializedSize() const noexcept
{
    size_t total_size = sizeof(uint64_t) + sizeof(id) + sizeof(create_timestamp_ms) + sizeof(last_modify_timestamp_ms) + sizeof(status);
    total_size += approximateSerializedSizeOf(
        ns, name, sql, created_by, last_modified_by, cron, checkpoint_init_values, target_database_name, target_table_name, sql);

    total_size += sizeof(checkpoint_columns.size());
    for (const auto & col : checkpoint_columns)
        total_size += approximateSerializedSizeOf(col);

    return total_size;
}

std::string TaskDescriptor::string() const
{
    return fmt::format(
        "data_version={} uuid={} name={} sql={} "
        "cron='{}' interval={}{} timeout={}{} "
        "checkpoint={} ",
        "target={}.{} "
        "status={} ",
        "create_timestamp={} last_modify_timestamp={} created_by={} last_modified_by={}",
        data_version,
        DB::toString(id),
        name,
        sql,
        cron,
        interval,
        interval_unit.toAbbreviation(),
        timeout,
        timeout_unit.toAbbreviation(),
        checkpoint_init_values,
        target_database_name,
        target_table_name,
        status,
        create_timestamp_ms,
        last_modify_timestamp_ms,
        created_by,
        last_modified_by);
}

bool TaskDescriptor::isValid() const noexcept
{
    return data_version > 0 && id != DB::UUIDHelpers::Nil && !ns.empty() && !name.empty() && !sql.empty();
}

}
