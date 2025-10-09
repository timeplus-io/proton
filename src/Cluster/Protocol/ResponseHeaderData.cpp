#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/ResponseHeaderData.h>

#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void ResponseHeaderData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    cluster::serializeEnumWide(response_opcode, wb);
    DB::writeVarUInt(response_version, wb);
    DB::writeVarUInt(correlation_id, wb);
    DB::writeVarInt(timestamp_ms, wb);

    /// auto header_version = requestHeaderVersion(response_opcode, response_version);
    /// In future, we can branch encoding according to header_version
}

void ResponseHeaderData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    response_opcode = cluster::deserializeEnumWide<protocol::OpCode>(rb);
    DB::readVarUInt(response_version, rb);

    /// auto header_version = requestHeaderVersion(response_opcode, response_version);
    /// In future, we can branch decoding according to header_version
    DB::readVarUInt(correlation_id, rb);
    DB::readVarInt(timestamp_ms, rb);
}

std::string ResponseHeaderData::doString() const
{
    return fmt::format(
        "response_opcode={} response_version={} correlation_id=0x{:x} timestamp_ms={}",
        opCodeString(),
        response_version,
        correlation_id,
        timestamp_ms);
}
}
