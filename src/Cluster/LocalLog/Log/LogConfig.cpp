#include <Cluster/LocalLog/Log/LogConfig.h>

#include <fmt/format.h>
#include <magic_enum.hpp>

namespace cluster::nlog
{
void LogConfig::validate()
{
    if (max_entry_size == 0)
        max_entry_size = DEFAULT_MAX_ENTRY_SIZE;

    if (segment_size == 0)
        segment_size = DEFAULT_SEGMENT_SIZE;

    /// per X milliseconds
    if (flush_interval_ms <= 0)
        flush_interval_ms = 0;

    if (retention_ms <= 0)
        retention_ms = 0;

    if (segment_ms == 0)
        segment_ms = DEFAULT_SEGMENT_MS;

    if (file_delete_delay_ms == 0)
        file_delete_delay_ms = DEFAULT_LOG_DELETE_DELAY_MS;

    if (index_interval_bytes == 0)
        index_interval_bytes = DEFAULT_INDEX_INTERVAL_BYTES;

    if (index_interval_entries == 0)
        index_interval_entries = DEFAULT_INDEX_INTERVAL_ENTRIES;

    if (max_cached_bytes_per_shard)
        max_cached_entries_per_shard = DEFAULT_MAX_CACHED_ENTRIES_PER_SHARD;

    if (min_size_to_keep == 0)
        min_size_to_keep = 2 * segment_size;

    if (max_cached_entries_per_shard)
        max_cached_bytes_per_shard = DEFAULT_MAX_CACHED_BYTES_PER_SHARD;

    if (hard_state_ckpt_log_size == 0 || hard_state_ckpt_log_size > 1024 * 1024)
        hard_state_ckpt_log_size = 1024 * 1024;

    if (leader_epoch_index_log_size == 0 || leader_epoch_index_log_size > 8 * 1024 * 1024)
        leader_epoch_index_log_size = 8 * 1024 * 1024;

    if (timestamp_index_log_size == 0 || timestamp_index_log_size > 8 * 1024 * 1024)
        timestamp_index_log_size = 8 * 1024 * 1024;

    if (log_position_index_log_size == 0 || log_position_index_log_size > 8 * 1024 * 1024)
        log_position_index_log_size = 8 * 1024 * 1024;
}

std::string LogConfig::string() const
{
    return fmt::format(
        "max_entry_size={} segment_size={} segment_ms={} flush_interval_entries={} flush_interval_ms={} flush_bytes={} retention_size={} "
        "retention_ms={} file_delete_delay_ms={} index_interval_bytes={} index_interval_entries={} max_cached_entries_per_shard={} "
        "max_cached_bytes_per_shard={} codec={} min_size_to_keep={} preallocate={} inmemory={} incremental_flush={} "
        "hard_state_ckpt_log_size={} "
        "hard_state_ckpt_log_preallocate={} leader_epoch_ckpt_log_size={} leader_epoch_ckpt_log_preallocate={} timestamp_index_log_size={} "
        "timestamp_index_log_preallocate={} log_position_index_log_size={} log_position_index_log_preallocate={}",
        max_entry_size,
        segment_size,
        segment_ms,
        flush_interval_entries,
        flush_interval_ms,
        flush_bytes,
        retention_size,
        retention_ms,
        file_delete_delay_ms,
        index_interval_bytes,
        index_interval_entries,
        max_cached_entries_per_shard,
        max_cached_bytes_per_shard,
        magic_enum::enum_name(codec),
        min_size_to_keep,
        preallocate,
        inmemory,
        incremental_flush,
        hard_state_ckpt_log_size,
        hard_state_ckpt_log_preallocate,
        leader_epoch_index_log_size,
        leader_epoch_index_log_preallocate,
        timestamp_index_log_size,
        timestamp_index_log_preallocate,
        log_position_index_log_size,
        log_position_index_log_preallocate);
}
}
