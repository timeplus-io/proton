#pragma once

#include <cstdint>
#include <string>

namespace DB
{
struct CheckpointConfig
{
    uint64_t last_access_ttl_sec = 604800;
    uint64_t last_access_check_interval_sec = 7200;
    uint64_t delete_grace_interval_sec = 60;
    uint64_t teardown_flush_timeout_sec = 60;

    /// configs for NativeLog based ckpt
    uint64_t log_max_entry_size = 16 * 1024 * 1024;
    uint64_t log_max_cached_entries = 0; /// 0 means no cache
    uint64_t log_flush_bytes = 1;
    uint64_t log_flush_interval_entries = 1;
    uint64_t log_segment_size = 1024 * 1024 * 1024ull;
    uint64_t log_retention_size = 1;
    uint64_t log_retention_ms = 86400000; /// 1 day
    uint64_t log_min_size_to_keep = 1ull;

    uint64_t interval_sec = 0;
    uint64_t min_interval_sec = 60;
    uint64_t light_state_interval_sec = 5;
    uint64_t heavy_state_size_threshold = 500 * 1024 * 1024; /// 500MB
    uint64_t heavy_state_interval = 900; /// 15 minutes

    std::string path = "/var/lib/proton/checkpoint/";
};
}
