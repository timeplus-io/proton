#include <Cluster/Protocol/CreateAccessEntityRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace cluster::protocol
{

void CreateAccessEntityRequestData::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    desc.serialize(wb, version);
    DB::writeBinary(replace_if_exists, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void CreateAccessEntityRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t version)
{
    desc.deserialize(rb, version);
    DB::readBinary(replace_if_exists, rb);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t CreateAccessEntityRequestData::approximateSerializedSize() const noexcept
{
    return desc.approximateSerializedSize() + sizeof(replace_if_exists) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string CreateAccessEntityRequestData::doString() const
{
    return fmt::format("{} replace_if_exists={} initiator=0x{:x} timeout_ms={}", desc.string(), replace_if_exists, initiator, timeout_ms);
}

}
