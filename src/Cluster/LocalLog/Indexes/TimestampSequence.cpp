#include <Cluster/LocalLog/Indexes/TimestampSequence.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::nlog
{
void TimestampSequence::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    /// We like TimestampSequence serialized size deterministic
    /// So avoid using writeVarUInt var encoding
    DB::writeBinary(timestamp, wb);
    DB::writeBinary(sn, wb);
}

void TimestampSequence::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readBinary(timestamp, rb);
    DB::readBinary(sn, rb);
}

std::string TimestampSequence::string() const
{
    return fmt::format("timestamp={} sn={}", timestamp, sn);
}

}
