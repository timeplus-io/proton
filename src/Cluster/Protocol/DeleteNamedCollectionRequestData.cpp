#include <Cluster/Protocol/DeleteNamedCollectionRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>


namespace cluster::protocol
{
void DeleteNamedCollectionRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(collection_name, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DeleteNamedCollectionRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(collection_name, rb);
    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t DeleteNamedCollectionRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(collection_name) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string DeleteNamedCollectionRequestData::doString() const
{
    return fmt::format("collection_name={} initiator=0x{:x} timeout_ms={}", collection_name, initiator, timeout_ms);
}
}
