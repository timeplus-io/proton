#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/DeleteUserDefinedFunctionRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void DeleteUserDefinedFunctionRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(func_name, wb);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DeleteUserDefinedFunctionRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(func_name, rb);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t DeleteUserDefinedFunctionRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(func_name) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string DeleteUserDefinedFunctionRequestData::doString() const
{
    return fmt::format("func_name={} initiator=0x{:x} timeout_ms={}", func_name, initiator, timeout_ms);
}

}
