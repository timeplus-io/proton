#pragma once

#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{
/// return the start timestamp in milli-seconds in UTC timezone or local timezone
/// or return sequence ID
/// Timestamp formats:
/// 1) absolute ISO8601: 2020-01-01T01:12:45.123+08:00,2020-01-01T01:12:48.123+08:00,2020-01-00T01:12:45.123+08:00
/// 2) relative time: -10m,-20m,-30m
/// Sequence ID formats:
/// 1) absolute sn: 1000,2000,3000,
/// 2) relative sn : latest,earliest,latest
/// @return : timestamp or sequence number for each shard,
/// and a bool to indicate if it is timestamp based seek : true timestamp seek otherwise sequence based seek
std::pair<bool, std::vector<int64_t>> parseSeekTo(const String & seek_to, Int32 shards, bool utc);

/// returns whether the storage supports streaming queries
bool supportStreamingQuery(const StoragePtr & storage);
}
