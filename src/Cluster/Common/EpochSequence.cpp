#include <Cluster/Common/EpochSequence.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void EpochSequence::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    /// We like LeaderEpochSequence serialized size deterministic
    /// So avoid using writeVarUInt var encoding
    DB::writeBinary(static_cast<uint64_t>(1), wb);
    DB::writeBinary(sn, wb);
}

void EpochSequence::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    uint64_t dummy_epoch;
    DB::readBinary(dummy_epoch, rb);
    /// epoch is always 1 in  mode
    DB::readBinary(sn, rb);
}

std::string EpochSequence::string() const
{
    return fmt::format("sn={}", sn);
}
}
