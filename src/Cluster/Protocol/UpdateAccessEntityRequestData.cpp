#include <Cluster/Protocol/UpdateAccessEntityRequestData.h>

#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{

void UpdateAccessEntityRequestData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    DB::writeVarUInt(version_before_update, wb);
    new_entity.serialize(wb, version);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void UpdateAccessEntityRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    DB::readVarUInt(version_before_update, rb);
    new_entity.deserialize(rb, version);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t UpdateAccessEntityRequestData::approximateSerializedSize() const noexcept
{
    return sizeof(version_before_update) + new_entity.approximateSerializedSize() + sizeof(initiator) + sizeof(timeout_ms);
}

std::string UpdateAccessEntityRequestData::doString() const
{
    return fmt::format(
        "version_before_update={} new_entry={{{}}} initiator=0x{:x} timeout_ms={}",
        version_before_update,
        new_entity.string(),
        initiator,
        timeout_ms);
}

}
