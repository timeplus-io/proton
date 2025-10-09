#include <Cluster/Common/Version.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void Version::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarUInt(major, wb);
    DB::writeVarUInt(minor, wb);
    DB::writeVarUInt(patch, wb);
    DB::writeVarUInt(internal, wb);
}

void Version::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readVarUInt(major, rb);
    DB::readVarUInt(minor, rb);
    DB::readVarUInt(patch, rb);
    DB::readVarUInt(internal, rb);
}

std::string Version::string() const
{
    return fmt::format("{}.{}.{}", major, minor, patch);
}
}
