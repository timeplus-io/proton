#pragma once

#include <base/types.h>

namespace DB
{
enum class SeekToType : uint8_t
{
    ABSOLUTE_TIME = 0,
    RELATIVE_TIME,
    SEQUENCE_NUMBER
};

struct SeekToInfo
{
    String seek_to;
    std::vector<Int64> seek_points;
    SeekToType type;

    explicit SeekToInfo(String seek_to_);

    SeekToInfo(String seek_to_, std::vector<Int64> seek_points_, SeekToType seek_type_)
        : seek_to(std::move(seek_to_)), seek_points(std::move(seek_points_)), type(seek_type_)
    {
    }

    const String & getSeekTo() const { return seek_to; }
    const std::vector<Int64> & getSeekPoints() const { return seek_points; }
    const SeekToType & getSeekToType() const { return type; }
    String getSeekToForSettings() const;

    bool isTimeBased() const { return type == SeekToType::ABSOLUTE_TIME || type == SeekToType::RELATIVE_TIME; }

    void replicateForShards(Int32 shards);

    /// return the start timestamp in milli-seconds in UTC timezone or local timezone
    /// or return sequence ID
    /// Timestamp formats:
    /// 1) absolute ISO8601: 2020-01-01T01:12:45.123+08:00,2020-01-01T01:12:48.123+08:00,2020-01-00T01:12:45.123+08:00
    /// 2) relative time: -10m,-20m,-30m
    /// Sequence ID formats:
    /// 1) absolute sn: 1000,2000,3000,
    /// 2) relative sn: latest,earliest,latest
    /// @return : timestamp or sequence number for each shard,
    /// and a bool to indicate if it is timestamp based seek : true timestamp seek otherwise sequence based seek
    static std::pair<SeekToType, std::vector<int64_t>> parse(const String & seek_to_str, bool utc);
};

using SeekToInfoPtr = std::shared_ptr<SeekToInfo>;
using SeekToInfos = std::vector<SeekToInfoPtr>;
using SeekToInfosOfStreams = std::unordered_map<size_t, SeekToInfos>;

std::pair<int64_t, bool> tryParseAbsoluteTimeSeek(const String & seek_to);
}
