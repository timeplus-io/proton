#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/CreateUserDefinedFunctionRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void CreateUserDefinedFunctionRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    desc.serialize(wb, Nulls::NullVersion);

    cluster::serializeEnum(exists_op, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void CreateUserDefinedFunctionRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    desc.deserialize(rb, Nulls::NullVersion);

    exists_op = cluster::deserializeEnum<ExistsOperation>(rb);
    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

std::string CreateUserDefinedFunctionRequestData::doString() const
{
    return fmt::format("{} initiator=0x{:x} timeout_ms={}", desc.string(), initiator, timeout_ms);
}

size_t CreateUserDefinedFunctionRequestData::approximateSerializedSize() const noexcept
{
    return desc.approximateSerializedSize() + sizeof(initiator) + sizeof(timeout_ms);
}

}
