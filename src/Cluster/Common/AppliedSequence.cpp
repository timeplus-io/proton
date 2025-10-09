#include <Cluster/Common/AppliedSequence.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void AppliedSequence::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarInt(sn, wb);
}

void AppliedSequence::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readVarInt(sn, rb);
}

std::string AppliedSequence::string() const
{
    return fmt::format("sn={}", sn);
}

}
