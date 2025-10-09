#include <Cluster/Protocol/DeleteAccessEntityRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{

void DeleteAccessEntityRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeBinary(id, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DeleteAccessEntityRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readBinary(id, rb);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t DeleteAccessEntityRequestData::approximateSerializedSize() const noexcept
{
    return sizeof(id) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string DeleteAccessEntityRequestData::doString() const
{
    return fmt::format("id={} initiator=0x{:x} timeout_ms={}", DB::toString(id), initiator, timeout_ms);
}

}
