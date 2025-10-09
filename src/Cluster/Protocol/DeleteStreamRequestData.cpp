#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/DeleteStreamRequestData.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::protocol
{
void DeleteStreamRequestData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeStringBinary(sql_ddl, wb);
    stream.serialize(wb, Nulls::NullVersion);

    DB::writeVarUInt(initiator, wb);
    DB::writeVarInt(timeout_ms, wb);
}

void DeleteStreamRequestData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(sql_ddl, rb);
    stream.deserialize(rb, Nulls::NullVersion);

    DB::readVarUInt(initiator, rb);
    DB::readVarInt(timeout_ms, rb);
}

std::string DeleteStreamRequestData::doString() const
{
    return fmt::format("sql_ddl={} {} initiator=0x{:x} timeout_ms={}", sql_ddl, stream.string(), initiator, timeout_ms);
}
}
