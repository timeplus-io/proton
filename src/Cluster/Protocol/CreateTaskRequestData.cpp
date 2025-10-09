#include <Cluster/Protocol/CreateTaskRequestData.h>

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void CreateTaskRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    desc.serialize(wb, Nulls::NullVersion);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);

    cluster::serializeEnum(exists_op, wb);
    DB::writeStringBinary(requested_by, wb);
    DB::writeVarInt(requested_ts, wb);
}

void CreateTaskRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    desc.deserialize(rb, Nulls::NullVersion);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);

    exists_op = cluster::deserializeEnum<ExistsOperation>(rb);
    DB::readStringBinary(requested_by, rb);
    DB::readVarInt(requested_ts, rb);
}

std::string CreateTaskRequestData::doString() const
{
    return fmt::format(
        "{} initiator=0x{:x} timeout_ms={} exists_op={} requested_by={} requested_ts={}",
        desc.string(),
        initiator,
        timeout_ms,
        magic_enum::enum_name(exists_op),
        requested_by,
        requested_ts);
}
}
