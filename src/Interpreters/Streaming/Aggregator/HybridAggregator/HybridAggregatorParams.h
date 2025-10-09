#pragma once

#include <Interpreters/Streaming/Aggregator/IAggregatorParams.h>

namespace DB::Streaming
{

struct HybridAggregatorParams final : public IAggregatorParams
{
    HybridAggregatorParams(
        const Names & keys_,
        const AggregateDescriptions & aggregates_,
        String spill_dir_path_,
        size_t max_hot_keys_count_,
        size_t max_block_size_,
        size_t max_threads_,
        bool compile_aggregate_expressions_,
        size_t min_count_to_compile_aggregate_expression_,
        GroupBy streaming_group_by_ = GroupBy::Other,
        ssize_t delta_col_pos_ = -1,
        TrackingUpdatesType tracking_updates_type_ = TrackingUpdatesType::None,
        bool enable_key_cache_ = true,
        size_t streaming_window_count_ = 0,
        size_t window_keys_num_ = 0,
        WindowParamsPtr window_params_ = nullptr,
        std::optional<EmitAfterKeyExpirationParams> emit_key_params_ = {})
        : IAggregatorParams(
              keys_,
              aggregates_,
              max_block_size_,
              delta_col_pos_,
              streaming_group_by_,
              tracking_updates_type_,
              enable_key_cache_,
              streaming_window_count_,
              window_keys_num_,
              std::move(window_params_),
              max_threads_,
              emit_key_params_,
              compile_aggregate_expressions_,
              min_count_to_compile_aggregate_expression_)
        , spill_dir_path(spill_dir_path_)
        , max_hot_key_count(max_hot_keys_count_)
    {
    }

    AggregatorType aggregatorType() const noexcept override { return AggregatorType::Hybrid; }

    String spill_dir_path;
    size_t max_hot_key_count;
};

using HybridAggregatorParamsPtr = std::shared_ptr<HybridAggregatorParams>;

}
