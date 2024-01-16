#include <Interpreters/Streaming/CachedBlockMetrics.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming
{
void CachedBlockMetrics::serialize(WriteBuffer & wb) const
{
    DB::writeBinary(total_blocks, wb);
    DB::writeBinary(total_metadata_bytes, wb);
    DB::writeBinary(total_data_bytes, wb);
    DB::writeBinary(gced_blocks, wb);
}

void CachedBlockMetrics::deserialize(ReadBuffer & rb)
{
    DB::readBinary(total_blocks, rb);
    DB::readBinary(total_metadata_bytes, rb);
    DB::readBinary(total_data_bytes, rb);
    DB::readBinary(gced_blocks, rb);
}
}
}
