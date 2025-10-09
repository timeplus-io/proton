#include <Cluster/Protocol/DeleteStoragePolicyRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>


namespace cluster::protocol
{
void DeleteStoragePolicyRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(name, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DeleteStoragePolicyRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(name, rb);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

std::string DeleteStoragePolicyRequestData::doString() const
{
    return fmt::format("storage policy={} initiator=0x{:x} timeout_ms={}", name, initiator, timeout_ms);
}
}
