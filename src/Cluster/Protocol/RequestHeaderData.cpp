#include <Cluster/Protocol/RequestHeaderData.h>

#include <Cluster/Common/serde.h>
#include <IO/WriteHelpers.h>

namespace cluster::protocol
{
void RequestHeaderData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    cluster::serializeEnumWide(request_opcode, wb);
    DB::writeVarUInt(request_version, wb);
    DB::writeVarUInt(correlation_id, wb);
    DB::writeVarInt(timestamp_ms, wb);

    /// auto header_version = requestHeaderVersion(request_opcode, request_version);
    /// In future, we can branch encoding according to header_version
}

void RequestHeaderData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    request_opcode = cluster::deserializeEnumWide<protocol::OpCode>(rb);
    DB::readVarUInt(request_version, rb);

    /// auto header_version = requestHeaderVersion(request_opcode, request_version);
    /// In future, we can branch decoding according to header_version
    DB::readVarUInt(correlation_id, rb);
    DB::readVarInt(timestamp_ms, rb);
}

std::string RequestHeaderData::doString() const
{
    return fmt::format(
        "request_opcode={} request_version={} correlation_id=0x{:x} timestamp_ms={}",
        magic_enum::enum_name(request_opcode),
        request_version,
        correlation_id,
        timestamp_ms);
}
}
