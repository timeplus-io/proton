#pragma once

#include <fmt/format.h>

namespace nlog
{
struct TailCacheConfig
{
    int64_t max_cached_entries = 10000;
    int64_t max_cached_bytes = 419430400;
    int64_t max_cached_entries_per_shard = 100;
    int64_t max_cached_bytes_per_shard = 4194304;

    std::string string() const
    {
        return fmt::format(
            "max_cached_entries={} max_cached_bytes={} max_cached_entries_per_shard={} max_cached_bytes_per_shard={}",
            max_cached_entries,
            max_cached_bytes,
            max_cached_entries_per_shard,
            max_cached_bytes_per_shard);
    }
};
}
