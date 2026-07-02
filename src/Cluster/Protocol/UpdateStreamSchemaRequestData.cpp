#include <Cluster/Protocol/StreamDescriptor.h>
#include <Cluster/Protocol/UpdateStreamSchemaRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace cluster::protocol
{
void UpdateStreamSchemaRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarUInt(version_before_update, wb);

    stream.serialize(wb, Nulls::NullVersion);

    DB::writeVectorBinary(alter_commands, wb);

    DB::writeStringBinary(sql_def, wb);

    DB::writeStringBinary(modified_by, wb);
    DB::writeVarInt(last_modified, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void UpdateStreamSchemaRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readVarUInt(version_before_update, rb);

    stream.deserialize(rb, Nulls::NullVersion);

    DB::readVectorBinary(alter_commands, rb);

    DB::readStringBinary(sql_def, rb);

    DB::readStringBinary(modified_by, rb);
    DB::readVarInt(last_modified, rb);
    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t UpdateStreamSchemaRequestData::approximateSerializedSize() const noexcept
{
    size_t cmd_size = sizeof(alter_commands.size());
    for (const auto & alter_command : alter_commands)
        cmd_size += approximateSerializedSizeOf(alter_command);

    return sizeof(version_before_update) + stream.approximateSerializedSize() + cmd_size + approximateSerializedSizeOf(sql_def, modified_by)
        + sizeof(last_modified) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string UpdateStreamSchemaRequestData::doString() const
{
    return fmt::format(
        "version_before_update=0x{:x} {} alter_commands=[{}] sql_def={} modified_by={} last_modified={} initiator=0x{:x} timeout_ms={}",
        version_before_update,
        stream.string(),
        fmt::join(alter_commands, ", "),
        sql_def,
        modified_by,
        last_modified,
        initiator,
        timeout_ms);
}

}
