#pragma once

#include "LogOffsetMetadata.h"

#include <string>
#include <vector>

namespace nlog
{
struct CompressionCodec
{
    uint8_t codec = 0;
    std::string name;
};

enum class LeaderHwChange : uint8_t
{
    INCREASED = 0,
    SAME,
    NONE,
};

struct RecordError
{
    uint32_t batch_index;
    std::string message;

    RecordError(uint32_t batch_index_, const std::string & message_) : batch_index(batch_index_), message(message_) { }
};

/// Struct to hold various quantities we compute about each message set before
/// appending to the log
struct LogAppendInfo
{
    /// The first offset in the message set. If the message is a duplicate
    /// message the segment base offset and relative position in segment
    /// will be unknown
    LogOffsetMetadata first_offset;

    /// The last offset in the message set
    int64_t last_offset;

    /// The shard leader epoch corresponding to the last offset, if available
    /// -1 unknown
    int32_t last_leader_epoch = -1;

    /// The maximum event timestamp of the message set
    int64_t max_timestamp;

    int64_t offset_of_max_timestamp = 0;

    /// The log append time of the message set
    int64_t append_timestamp = -1;

    /// The start offset of the log at the time of this append
    int64_t log_start_offset;

    /// The source codec used in the message set (send by the producer)
    CompressionCodec source_codec;

    /// The target codec of the message set (after applying the broker compression configuration if any)
    CompressionCodec target_codec;

    /// The number of valid bytes
    int32_t valid_bytes;

    LeaderHwChange leader_hw_change = LeaderHwChange::NONE;

    std::vector<RecordError> record_errors;
    std::string error_message;
};
}
