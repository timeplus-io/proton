#include <Interpreters/Streaming/joinMetrics.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming
{
void JoinMetrics::serialize(WriteBuffer & wb) const
{
    DB::writeBinary(current_total_blocks, wb);
    DB::writeBinary(current_total_bytes, wb);
    DB::writeBinary(total_blocks, wb);
    DB::writeBinary(total_bytes, wb);
    DB::writeBinary(gced_blocks, wb);
}

void JoinMetrics::deserialize(ReadBuffer & rb)
{
    DB::readBinary(current_total_blocks, rb);
    DB::readBinary(current_total_bytes, rb);
    DB::readBinary(total_blocks, rb);
    DB::readBinary(total_bytes, rb);
    DB::readBinary(gced_blocks, rb);
}

void JoinGlobalMetrics::serialize(WriteBuffer & wb) const
{
    DB::writeBinary(total_join, wb);
    DB::writeBinary(left_block_and_right_range_bucket_no_intersection_skip, wb);
    DB::writeBinary(right_block_and_left_range_bucket_no_intersection_skip, wb);
}

void JoinGlobalMetrics::deserialize(ReadBuffer & rb)
{
    DB::readBinary(total_join, rb);
    DB::readBinary(left_block_and_right_range_bucket_no_intersection_skip, rb);
    DB::readBinary(right_block_and_left_range_bucket_no_intersection_skip, rb);
}

}
}
