#pragma once

#include <Common/Exception.h>

#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace nlog
{
struct LogSequenceMetadata
{
    int64_t record_sn;
    int64_t segment_base_sn;
    /// Physical position in segment file
    int64_t file_position;

    static const int64_t UNKNOWN_SEQUENCE = -1;
    static const int64_t UNKNOWN_FILE_POSITION = -1;

    LogSequenceMetadata() : record_sn(UNKNOWN_SEQUENCE) { }

    LogSequenceMetadata(
        int64_t record_sn_, int64_t segment_base_sn_ = UNKNOWN_SEQUENCE, int64_t file_position_ = UNKNOWN_FILE_POSITION)
        : record_sn(record_sn_), segment_base_sn(segment_base_sn_), file_position(file_position_)
    {
    }

    std::string string() const
    {
        return fmt::format(
            "record_sn={}, segment_base_sn={}, file_position={}", record_sn, segment_base_sn, file_position);
    }

    /// Check if this offset is already on an older segment compared with the given offset
    bool onOldSegment(const LogSequenceMetadata & rhs) const
    {
        if (sequenceOnly())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot compare its segment info since it only has record sequence info");

        return segment_base_sn < rhs.segment_base_sn;
    }

    /// Check if this offset is already on an older segment compared with the given offset
    bool onSameSegment(const LogSequenceMetadata & rhs) const
    {
        if (sequenceOnly())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot compare its segment info since it only has message offset info");

        return segment_base_sn == rhs.segment_base_sn;
    }

    bool sequenceOnly() const
    {
        return record_sn != UNKNOWN_SEQUENCE && segment_base_sn == UNKNOWN_SEQUENCE && file_position == UNKNOWN_FILE_POSITION;
    }

    bool isValid() const
    {
        return record_sn != UNKNOWN_SEQUENCE || segment_base_sn != UNKNOWN_SEQUENCE || file_position != UNKNOWN_FILE_POSITION;
    }
};
}
