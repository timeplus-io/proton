#include <Cluster/Common/FetchHint.h>

#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{

void FetchHint::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarInt(sn, wb);
    DB::writeVarInt(segment_base_sn, wb);
    DB::writeVarUInt(file_position, wb);
}

void FetchHint::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readVarInt(sn, rb);
    DB::readVarInt(segment_base_sn, rb);
    DB::readVarUInt(file_position, rb);
}

std::string FetchHint::string() const
{
    return fmt::format("sn={} segment_base_sn={} file_position={}", sn, segment_base_sn, file_position);
}
}
