#pragma once

namespace DB::Streaming
{
struct AggregatedDataMetrics
{
    size_t total_aggregated_rows = 0;
    size_t total_bytes_in_arena = 0;
    size_t hash_buffer_bytes = 0;
    size_t hash_buffer_cells = 0;
    size_t total_bytes_of_aggregate_states = 0;

    String string() const
    {
        return fmt::format(
            "total_aggregated_rows={} total_aggregated_bytes={} (total_bytes_in_arena: {}, hash_buffer_bytes: {}, hash_buffer_cells: {}, "
            "total_bytes_of_aggregate_states: {})",
            total_aggregated_rows,
            total_bytes_in_arena + hash_buffer_bytes,
            total_bytes_in_arena,
            hash_buffer_bytes,
            hash_buffer_cells,
            total_bytes_of_aggregate_states);
    }
};
}
