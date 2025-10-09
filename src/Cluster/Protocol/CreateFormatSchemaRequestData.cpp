#include <Cluster/Protocol/CreateFormatSchemaRequestData.h>

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void CreateFormatSchemaRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    desc.serialize(wb, Nulls::NullVersion);

    cluster::serializeEnum(exists_op, wb);
    DB::writeStringBinary(requested_by, wb);
    DB::writeVarInt(requested_ts, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void CreateFormatSchemaRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    desc.deserialize(rb, Nulls::NullVersion);

    exists_op = cluster::deserializeEnum<ExistsOperation>(rb);
    DB::readStringBinary(requested_by, rb);
    DB::readVarInt(requested_ts, rb);
    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

std::string CreateFormatSchemaRequestData::doString() const
{
    return fmt::format("{} executed_by={} initiator=0x{:x} timeout_ms={}", desc.string(), requested_by, initiator, timeout_ms);
}
}
