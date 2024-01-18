#include <Interpreters/Streaming/CachedBlockMetrics.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming
{
void CachedBlockMetrics::serialize(WriteBuffer & wb, VersionType version) const
{
    assert(version <= HAS_STATE_MAX_VERSION);
    DB::writeBinary(total_blocks, wb);
    DB::writeBinary(total_data_bytes, wb);
    DB::writeBinary(total_blocks, wb);
    DB::writeBinary(total_data_bytes, wb);
    DB::writeBinary(gced_blocks, wb);
}

void CachedBlockMetrics::deserialize(ReadBuffer & rb, VersionType version)
{
    assert(version <= HAS_STATE_MAX_VERSION);
    /// V1 layout [current_total_blocks, current_total_bytes, total_blocks, total_bytes, gced_blocks]
    [[maybe_unused]] size_t temp;
    DB::readBinary(temp, rb);
    DB::readBinary(temp, rb);
    DB::readBinary(temp, rb);
    DB::readBinary(temp, rb);
    DB::readBinary(temp, rb);
}
}
}
