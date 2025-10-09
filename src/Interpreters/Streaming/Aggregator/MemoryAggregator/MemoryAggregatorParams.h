#pragma once

#include <Interpreters/Streaming/Aggregator/IAggregatorParams.h>
#include <Interpreters/TemporaryDataOnDisk.h>

namespace DB::Streaming
{

struct MemoryAggregatorParams final : public IAggregatorParams
{
    /// The settings of approximate calculation of GROUP BY.
    /// Do we need to put into AggregatedDataVariants::without_key aggregates for keys that are not in max_rows_to_group_by.
    const bool overflow_row;
    const size_t max_rows_to_group_by;
    const OverflowMode group_by_overflow_mode;

    /// Two-level aggregation settings (used for a large number of keys).
    /** With how many keys or the size of the aggregation state in bytes,
          *  two-level aggregation begins to be used. Enough to reach of at least one of the thresholds.
          * 0 - the corresponding threshold is not specified.
          */
    size_t group_by_two_level_threshold;
    size_t group_by_two_level_threshold_bytes;

    /// Settings to flush temporary data to the filesystem (external aggregation).
    const size_t max_bytes_before_external_group_by; /// 0 - do not use external aggregation.

    /// Return empty result when aggregating without keys on empty set.
    bool empty_result_for_aggregation_by_empty_set;

    TemporaryDataOnDiskScopePtr tmp_data_scope;

    const size_t min_free_disk_space;

    MemoryAggregatorParams(
        const Names & keys_,
        const AggregateDescriptions & aggregates_,
        bool overflow_row_,
        size_t max_rows_to_group_by_,
        OverflowMode group_by_overflow_mode_,
        size_t group_by_two_level_threshold_,
        size_t group_by_two_level_threshold_bytes_,
        size_t max_bytes_before_external_group_by_,
        bool empty_result_for_aggregation_by_empty_set_,
        TemporaryDataOnDiskScopePtr tmp_data_scope_,
        size_t max_threads_,
        size_t min_free_disk_space_,
        bool compile_aggregate_expressions_,
        size_t min_count_to_compile_aggregate_expression_,
        size_t max_block_size_,
        size_t streaming_window_count_ = 0,
        GroupBy streaming_group_by_ = GroupBy::Other,
        ssize_t delta_col_pos_ = -1,
        size_t window_keys_num_ = 0,
        WindowParamsPtr window_params_ = nullptr,
        TrackingUpdatesType tracking_updates_type_ = TrackingUpdatesType::None,
        bool enable_keys_cache_ = true)
        : IAggregatorParams(
              keys_,
              aggregates_,
              max_block_size_,
              delta_col_pos_,
              streaming_group_by_,
              tracking_updates_type_,
              enable_keys_cache_,
              streaming_window_count_,
              window_keys_num_,
              std::move(window_params_),
              max_threads_,
              std::optional<EmitAfterKeyExpirationParams>{},
              compile_aggregate_expressions_,
              min_count_to_compile_aggregate_expression_)
        , overflow_row(overflow_row_)
        , max_rows_to_group_by(max_rows_to_group_by_)
        , group_by_overflow_mode(group_by_overflow_mode_)
        , group_by_two_level_threshold(group_by_two_level_threshold_)
        , group_by_two_level_threshold_bytes(group_by_two_level_threshold_bytes_)
        , max_bytes_before_external_group_by(max_bytes_before_external_group_by_)
        , empty_result_for_aggregation_by_empty_set(empty_result_for_aggregation_by_empty_set_)
        , tmp_data_scope(std::move(tmp_data_scope_))
        , min_free_disk_space(min_free_disk_space_)
    {
    }

    /// Only parameters that matter during merge.
    MemoryAggregatorParams(
        const Names & keys_, const AggregateDescriptions & aggregates_, bool overflow_row_, size_t max_threads_, size_t max_block_size_)
        : MemoryAggregatorParams(
              keys_,
              aggregates_,
              overflow_row_,
              0,
              OverflowMode::THROW,
              0,
              0,
              0,
              false,
              nullptr,
              max_threads_,
              0,
              false,
              0,
              max_block_size_,
              true)
    {
    }

    AggregatorType aggregatorType() const noexcept override { return AggregatorType::Memory; }
};

using MemoryAggregatorParamsPtr = std::shared_ptr<MemoryAggregatorParams>;

}
