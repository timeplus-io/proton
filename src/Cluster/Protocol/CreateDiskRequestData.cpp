#include <Cluster/Protocol/CreateDiskRequestData.h>

#include <IO/WriteHelpers.h>

#include <fmt/format.h>


namespace cluster::protocol
{
void CreateDiskRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    chassert(disk);
    disk->serialize(wb, /*version=*/1);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void CreateDiskRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    disk = std::make_shared<DiskDescriptor>();
    disk->deserialize(rb, /*version=*/1);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t CreateDiskRequestData::approximateSerializedSize() const noexcept
{
    chassert(disk);
    return disk->approximateSerializedSize() + sizeof(initiator) + sizeof(timeout_ms);
}

std::string CreateDiskRequestData::doString() const
{
    return fmt::format("disk={{{}}} initiator=0x{:x} timeout_ms={}", disk ? disk->string() : "null", initiator, timeout_ms);
}

}
