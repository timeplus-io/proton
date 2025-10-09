#include <Cluster/LocalLog/Indexes/SequencePosition.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster::nlog
{
void SequencePosition::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    /// We like SequencePosition serialized size deterministic
    /// So avoid using writeVarUInt var encoding
    DB::writeBinary(sn, wb);
    DB::writeBinary(file_position, wb);
}

void SequencePosition::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readBinary(sn, rb);
    DB::readBinary(file_position, rb);
}

std::string SequencePosition::string() const
{
    return fmt::format("sn={} file_position={}", sn, file_position);
}

}
