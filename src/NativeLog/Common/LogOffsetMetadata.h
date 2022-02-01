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
struct LogOffsetMetadata
{
    int64_t message_offset;
    int64_t segment_base_offset;
    /// Physical position in segment file
    int64_t file_position;

    static const int64_t UNKNOWN_OFFSET = -1;
    static const int64_t UNKNOWN_FILE_POSITION = -1;

    LogOffsetMetadata() : message_offset(UNKNOWN_OFFSET) { }

    LogOffsetMetadata(
        int64_t message_offset_, int64_t segment_base_offset_ = UNKNOWN_OFFSET, int64_t file_position_ = UNKNOWN_FILE_POSITION)
        : message_offset(message_offset_), segment_base_offset(segment_base_offset_), file_position(file_position_)
    {
    }

    std::string string() const
    {
        return fmt::format(
            "message_offset={}, segment_base_offset={}, file_position={}", message_offset, segment_base_offset, file_position);
    }

    /// Check if this offset is already on an older segment compared with the given offset
    bool onOldSegment(const LogOffsetMetadata & rhs) const
    {
        if (messageOffsetOnly())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot compare its segment info since it only has message offset info");

        return segment_base_offset < rhs.segment_base_offset;
    }

    /// Check if this offset is already on an older segment compared with the given offset
    bool onSameSegment(const LogOffsetMetadata & rhs) const
    {
        if (messageOffsetOnly())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot compare its segment info since it only has message offset info");

        return segment_base_offset == rhs.segment_base_offset;
    }

    bool messageOffsetOnly() const
    {
        return message_offset != UNKNOWN_OFFSET && segment_base_offset == UNKNOWN_OFFSET && file_position == UNKNOWN_FILE_POSITION;
    }

    bool isValid() const
    {
        return message_offset != UNKNOWN_OFFSET || segment_base_offset != UNKNOWN_OFFSET || file_position != UNKNOWN_FILE_POSITION;
    }
};
}
