#include <Cluster/Protocol/GetNamedCollectionRequestData.h>

#include <Cluster/Common/utils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>


namespace cluster::protocol
{
void GetNamedCollectionRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(collection_name, wb);
    DB::writeVarUInt(versions_requested, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void GetNamedCollectionRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(collection_name, rb);
    DB::readVarUInt(versions_requested, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t GetNamedCollectionRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(collection_name) + sizeof(versions_requested) + sizeof(initiator) + sizeof(consistent_read)
        + sizeof(timeout_ms);
}

std::string GetNamedCollectionRequestData::doString() const
{
    return fmt::format(
        "collection_name={} versions_requested={} initiator=0x{:x} consistent_read={} timeout_ms={}",
        collection_name,
        versions_requested,
        initiator,
        consistent_read,
        timeout_ms);
}

}
