#include <Interpreters/Streaming/CachedBlockMetrics.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming
{
void CachedBlockMetrics::serialize(WriteBuffer & wb, VersionType version) const
{
    assert(version <= SERDE_REQUIRED_MAX_VERSION);
    DB::writeBinary(total_blocks, wb);
    DB::writeBinary(total_data_bytes, wb);
    DB::writeBinary(total_blocks, wb);
    DB::writeBinary(total_data_bytes, wb);
    DB::writeBinary(gced_blocks, wb);
}

void CachedBlockMetrics::deserialize(ReadBuffer & rb, VersionType version)
{
    assert(version <= SERDE_REQUIRED_MAX_VERSION);
    /// V1 layout [current_total_blocks, current_total_bytes, total_blocks, total_bytes, gced_blocks]
    DB::readBinary(total_blocks, rb);
    DB::readBinary(total_data_bytes, rb);
    DB::readBinary(total_blocks, rb);
    DB::readBinary(total_data_bytes, rb);
    DB::readBinary(gced_blocks, rb);
}
}
}
