#include <Cluster/LocalLog/Common/LogSequenceMetadata.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace cluster::nlog
{

void LogSequenceMetadata::serialize(DB::WriteBuffer & wb) const
{
    DB::writeVarInt(entry_sn, wb);

    if (segment_base_sn)
    {
        DB::writeBinary(static_cast<uint8_t>(1), wb);
        DB::writeVarInt(segment_base_sn.value(), wb);
    }
    else
    {
        DB::writeBinary(static_cast<uint8_t>(0), wb);
    }

    if (file_position)
    {
        DB::writeBinary(static_cast<uint8_t>(1), wb);
        DB::writeVarUInt(file_position.value(), wb);
    }
    else
    {
        DB::writeBinary(static_cast<uint8_t>(0), wb);
    }
}

void LogSequenceMetadata::deserialize(DB::ReadBuffer & rb)
{
    DB::readVarInt(entry_sn, rb);

    uint8_t has_segment_base_sn = 0;
    DB::readBinary(has_segment_base_sn, rb);

    if (has_segment_base_sn)
    {
        int64_t seg_base_sn = 0;
        DB::readVarInt(seg_base_sn, rb);
        segment_base_sn = seg_base_sn;
    }

    uint8_t has_file_position = 0;
    DB::readBinary(has_file_position, rb);

    if (has_file_position)
    {
        uint64_t file_pos = 0;
        DB::readVarUInt(file_pos, rb);
        file_position = file_pos;
    }
}

std::string LogSequenceMetadata::string() const
{
    return fmt::format(
        "log_entry_sn={}, segment_base_sn={}, file_position={}",
        entry_sn,
        segment_base_sn ? *segment_base_sn : 0,
        file_position ? *file_position : 0);
}

}
