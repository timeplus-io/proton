#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/UpdateStreamSettingsRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void UpdateStreamSettingsRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarUInt(version_before_update, wb);
    DB::writeVarUInt(flags, wb);

    DB::writeVectorBinary(alter_commands, wb);

    stream.serialize(wb, Nulls::NullVersion);

    DB::writeStringBinary(sql_def, wb);

    DB::writeVarUInt(flush_settings.size(), wb);
    for (const auto & flush_setting : flush_settings)
    {
        DB::writeStringBinary(flush_setting.first, wb);
        DB::writeVarInt(flush_setting.second, wb);
    }

    DB::writeVarUInt(retention_settings.size(), wb);
    for (const auto & retention_setting : retention_settings)
    {
        DB::writeStringBinary(retention_setting.first, wb);
        DB::writeVarInt(retention_setting.second, wb);
    }

    DB::writeStringBinary(modified_by, wb);
    DB::writeVarInt(last_modified, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void UpdateStreamSettingsRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readVarUInt(version_before_update, rb);
    DB::readVarUInt(flags, rb);

    DB::readVectorBinary(alter_commands, rb);

    stream.deserialize(rb, Nulls::NullVersion);

    DB::readStringBinary(sql_def, rb);

    uint64_t size = 0;
    DB::readVarUInt(size, rb);
    flush_settings.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        std::string key;
        int32_t value;
        DB::readStringBinary(key, rb);
        DB::readVarInt(value, rb);

        [[maybe_unused]] auto [_, inserted] = flush_settings.emplace(std::move(key), value);
        assert(inserted);
    }

    size = 0;
    DB::readVarUInt(size, rb);
    retention_settings.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        std::string key;
        int64_t value;
        DB::readStringBinary(key, rb);
        DB::readVarInt(value, rb);

        [[maybe_unused]] auto [_, inserted] = retention_settings.emplace(std::move(key), value);
        assert(inserted);
    }

    DB::readStringBinary(modified_by, rb);
    DB::readVarInt(last_modified, rb);
    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t UpdateStreamSettingsRequestData::approximateSerializedSize() const noexcept
{
    size_t cmd_size = sizeof(alter_commands.size());
    for (const auto & alter_command : alter_commands)
        cmd_size += approximateSerializedSizeOf(alter_command);

    size_t size = sizeof(version_before_update) + sizeof(flags) + cmd_size + approximateSerializedSizeOf(sql_def, modified_by)
        + stream.approximateSerializedSize();

    for (const auto & flush_setting : flush_settings)
        size += approximateSerializedSizeOf(flush_setting.first) + sizeof(flush_setting.second);

    for (const auto & retention_setting : retention_settings)
        size += approximateSerializedSizeOf(retention_setting.first) + sizeof(retention_setting.second);

    size += sizeof(flush_settings.size()) + sizeof(retention_settings.size()) + sizeof(initiator) + sizeof(timeout_ms);

    return size;
}

std::string UpdateStreamSettingsRequestData::doString() const
{
    return fmt::format(
        "version=0x{:x} flags=0x{:x} {} alter_commands=[{}] sql_def={} modified_by={} initiator=0x{:x} timeout_ms={}",
        version_before_update,
        flags,
        stream.string(),
        fmt::join(alter_commands, ", "),
        sql_def,
        modified_by,
        initiator,
        timeout_ms);
}
}
