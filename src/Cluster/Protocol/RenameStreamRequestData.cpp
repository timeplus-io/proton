#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/RenameStreamRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void RenameStreamRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(sql_ddl, wb);
    DB::writeStringBinary(ns, wb);
    DB::writeStringBinary(stream, wb);
    DB::writeStringBinary(new_stream, wb);

    DB::writeStringBinary(modified_by, wb);
    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void RenameStreamRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(sql_ddl, rb);
    DB::readStringBinary(ns, rb);
    DB::readStringBinary(stream, rb);
    DB::readStringBinary(new_stream, rb);

    DB::readStringBinary(modified_by, rb);
    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

size_t RenameStreamRequestData::approximateSerializedSize() const noexcept
{
    return approximateSerializedSizeOf(sql_ddl, ns, stream, new_stream, modified_by) + sizeof(initiator) + sizeof(timeout_ms);
}

std::string RenameStreamRequestData::doString() const
{
    return fmt::format(
        "sql_ddl={} ns={} stream={} new_stream={} modified_by={} initiator=0x{:x} timeout_ms={}",
        sql_ddl,
        ns,
        stream,
        new_stream,
        modified_by,
        initiator,
        timeout_ms);
}
}
