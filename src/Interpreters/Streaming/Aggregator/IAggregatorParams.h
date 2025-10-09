#pragma once

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Streaming/Aggregator/AggregatorType.h>
#include <Interpreters/Streaming/Aggregator/TrackingUpdatesData.h>
#include <Interpreters/Streaming/WindowCommon.h>

namespace DB::Streaming
{

struct EmitAfterKeyExpirationParams
{
    size_t key_ts_col_pos = 0;
    /// Normalized, the span interval can be in milliseconds, microseconds, nanoseconds or seconds
    /// precision depending on `identified by ts` column's precision
    uint64_t key_max_span_interval = 0;
    uint64_t timeout_interval_ms = 0;
    bool only_max_span = false;
    uint64_t max_tracking_keys = 0;
};

struct IAggregatorParams
{
    /// GroupBy tells if the first group column is either WindowStart or WindowEnd or
    /// anything else
    enum class GroupBy
    {
        WindowStart,
        WindowEnd,
        UserDefined,
        Other,
    };

    IAggregatorParams(
        const Names & keys_,
        const AggregateDescriptions & aggregates_,
        size_t max_block_size_,
        ssize_t delta_col_pos_,
        GroupBy streaming_group_by_,
        TrackingUpdatesType tracking_updates_type_,
        bool enable_keys_cache_,
        size_t streaming_window_count_,
        size_t window_keys_num_,
        WindowParamsPtr window_params_,
        size_t max_threads_,
        std::optional<EmitAfterKeyExpirationParams> emit_key_params_,
        bool compile_aggregate_expressions_,
        size_t min_count_to_compile_aggregate_expression_)
        : keys(keys_)
        , aggregates(aggregates_)
        , keys_size(keys.size())
        , aggregates_size(aggregates.size())
        , max_block_size(max_block_size_)
        , delta_col_pos(delta_col_pos_)
        , group_by(streaming_group_by_)
        , tracking_updates_type(tracking_updates_type_)
        , enable_keys_cache(enable_keys_cache_)
        , streaming_window_count(streaming_window_count_)
        , window_keys_num(window_keys_num_)
        , window_params(std::move(window_params_))
        , max_threads(max_threads_)
        , emit_key_params(emit_key_params_)
        , compile_aggregate_expressions(compile_aggregate_expressions_)
        , min_count_to_compile_aggregate_expression(min_count_to_compile_aggregate_expression_)
    {
    }

    IAggregatorParams(const IAggregatorParams &) = default;
    IAggregatorParams(IAggregatorParams &&) = default;
    virtual ~IAggregatorParams() = default;

    virtual AggregatorType aggregatorType() const noexcept = 0;

    std::string_view aggregatorName() const
    {
        switch (aggregatorType())
        {
            case AggregatorType::Memory:
                return "MemoryAggregator";
            case AggregatorType::Hybrid:
                return "HybridAggregator";
        }
    }

    static Block getHeader(const Block & header_, const Names & keys_, const AggregateDescriptions & aggregates_, bool final_);

    Block getHeader(const Block & header_, bool final_) const { return getHeader(header_, keys, aggregates, final_); }

    /// Returns keys and aggregated for EXPLAIN query
    void explain(WriteBuffer & out, size_t indent) const;
    void explain(JSONBuilder::JSONMap & map) const;

    /// For two level aggregation, the first key is always the window_start or window_end bucket column.
    /// This is handled by InterpreterSelectQuery
    const Names keys;
    const AggregateDescriptions aggregates;
    const size_t keys_size;
    const size_t aggregates_size;

    size_t max_block_size;
    ssize_t delta_col_pos;

    GroupBy group_by = GroupBy::Other;

    TrackingUpdatesType tracking_updates_type;
    bool enable_keys_cache = true;

    /// How many streaming windows to keep from recycling
    size_t streaming_window_count = 0;

    size_t window_keys_num;

    WindowParamsPtr window_params;

    size_t max_threads = 0;

    std::optional<EmitAfterKeyExpirationParams> emit_key_params;
    bool compile_aggregate_expressions;
    size_t min_count_to_compile_aggregate_expression;
};

using IAggregatorParamsPtr = std::shared_ptr<IAggregatorParams>;

}
