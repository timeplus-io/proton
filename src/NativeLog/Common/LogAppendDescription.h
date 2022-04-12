#pragma once

#include "LogSequenceMetadata.h"

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

/// Struct to hold various quantities we compute about each record before
/// appending to the log
struct LogAppendDescription
{
    /// The first offset in the message set. If the message is a duplicate
    /// message the segment base offset and relative position in segment
    /// will be unknown
    LogSequenceMetadata seq_metadata;

    /// The maximum event timestamp of the message set
    int64_t max_event_timestamp;

    /// The minimum event timestamp of the message set
    int64_t min_event_timestamp;

    /// The log append time of the message set
    int64_t append_timestamp = -1;

    /// The start sn of the log at the time of this append
    int64_t log_start_sn;

    /// The source codec used in the message set (send by the producer)
    CompressionCodec source_codec;

    /// The target codec of the message set (after applying the broker compression configuration if any)
    CompressionCodec target_codec;

    /// The number of valid bytes
    int32_t valid_bytes;

    LeaderHwChange leader_hw_change = LeaderHwChange::NONE;

    /// The shard leader epoch corresponding to the last offset, if available
    /// -1 unknown
    int32_t last_leader_epoch = -1;

    int32_t error_code = 0;
    std::string error_message;
};
}
