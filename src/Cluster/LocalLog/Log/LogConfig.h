#pragma once

#include <Cluster/Common/Stream.h>

#include <Compression/CompressionInfo.h>

#include <cstdint>
#include <memory>
#include <string>

namespace cluster::nlog
{
struct LogConfig
{
    static const uint64_t DEFAULT_MAX_ENTRY_SIZE = 10 * 1024 * 1024ull;
    static const uint64_t DEFAULT_SEGMENT_SIZE = 2ull * 1024 * 1024 * 1024;
    static const uint64_t DEFAULT_SEGMENT_MS = 24ll * 7 * 60 * 60 * 1000;
    static const uint64_t DEFAULT_FLUSH_INTERVAL_ENTRIES = 1000ull;
    static const int64_t DEFAULT_FLUSH_MS = 2 * 60 * 1000ll;
    static const uint64_t DEFAULT_FLUSH_BYTES = 8 * 1024 * 1024ull;
    static const uint64_t DEFAULT_RETENTION_BYTES = 1ull;
    static const uint64_t DEFAULT_RETENTION_MS = 24ull * 60 * 60 * 1000;
    static const uint64_t DEFAULT_LOG_DELETE_DELAY_MS = 300 * 1000ull;
    static const uint64_t DEFAULT_INDEX_INTERVAL_BYTES = 4096 * 1024ull;
    static const uint64_t DEFAULT_INDEX_INTERVAL_ENTRIES = 1000ull;
    static const uint64_t DEFAULT_MAX_CACHED_ENTRIES_PER_SHARD = 100ull;
    static const uint64_t DEFAULT_MAX_CACHED_BYTES_PER_SHARD = 4194304ull;

    uint64_t max_entry_size = DEFAULT_MAX_ENTRY_SIZE;
    uint64_t segment_size = DEFAULT_SEGMENT_SIZE;
    int64_t segment_ms = DEFAULT_SEGMENT_MS;
    /// per X entries
    uint64_t flush_interval_entries = DEFAULT_FLUSH_INTERVAL_ENTRIES;
    /// per X milliseconds
    int64_t flush_interval_ms = DEFAULT_FLUSH_MS;
    /// per X bytes
    int64_t flush_bytes = DEFAULT_FLUSH_BYTES;
    uint64_t retention_size = DEFAULT_RETENTION_BYTES;
    int64_t retention_ms = DEFAULT_RETENTION_MS;

    uint64_t file_delete_delay_ms = DEFAULT_LOG_DELETE_DELAY_MS;
    uint64_t index_interval_bytes = DEFAULT_INDEX_INTERVAL_BYTES;
    uint64_t index_interval_entries = DEFAULT_INDEX_INTERVAL_ENTRIES;

    uint64_t max_cached_entries_per_shard = DEFAULT_MAX_CACHED_ENTRIES_PER_SHARD;
    uint64_t max_cached_bytes_per_shard = DEFAULT_MAX_CACHED_BYTES_PER_SHARD;
    uint64_t min_size_to_keep = segment_size * 2;
    uint64_t hard_state_ckpt_log_size = 1024 * 1024;
    bool hard_state_ckpt_log_preallocate = true;
    uint64_t leader_epoch_index_log_size = 8 * 1024 * 1024;
    bool leader_epoch_index_log_preallocate = true;
    uint64_t timestamp_index_log_size = 8 * 1024 * 1024;
    bool timestamp_index_log_preallocate = true;
    uint64_t log_position_index_log_size = 8 * 1024 * 1024;
    bool log_position_index_log_preallocate = true;

    DB::CompressionMethodByte codec = DB::CompressionMethodByte::NONE;
    bool preallocate = true;
    bool inmemory = false;
    bool incremental_flush = false;

    void validate();

    std::shared_ptr<LogConfig> clone() const { return std::make_shared<LogConfig>(*this); }

    std::string string() const;
};

using LogConfigPtr = std::shared_ptr<LogConfig>;
}
