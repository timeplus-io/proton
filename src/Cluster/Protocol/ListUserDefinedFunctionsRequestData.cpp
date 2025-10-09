#include <Cluster/Protocol/ListUserDefinedFunctionsRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void ListUserDefinedFunctionsRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(func_name, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeBinary(consistent_read, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void ListUserDefinedFunctionsRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(func_name, rb);

    DB::readVarUInt(initiator, rb);
    DB::readBinary(consistent_read, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t ListUserDefinedFunctionsRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(func_name) + sizeof(initiator) + sizeof(consistent_read) + sizeof(timeout_ms);
}

std::string ListUserDefinedFunctionsRequestData::doString() const
{
    return fmt::format("func_name={} initiator=0x{:x} consistent_read={} timeout_ms={}", func_name, initiator, consistent_read, timeout_ms);
}

}
