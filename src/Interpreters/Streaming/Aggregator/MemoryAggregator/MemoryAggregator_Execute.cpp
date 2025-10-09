#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>

#include <Interpreters/CompiledAggregateFunctionsHolder.h>

namespace DB
{

namespace Streaming
{

/// return {should_abort, need_finalization}
std::pair<bool, bool> MemoryAggregator::executeOnBlock(
    const Block & block, MemoryAggregatedDataVariants & result, ColumnRawPtrs & key_columns, AggregateColumns & aggregate_columns) const
{
    return executeOnBlock(
        block.getColumns(),
        /*row_begin=*/0,
        block.rows(),
        result,
        key_columns,
        aggregate_columns,
        /*new_keys=*/false);
}

/// return {should_abort, need_finalization}
std::pair<bool, bool> MemoryAggregator::executeOnBlock(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & variant_result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    bool /*new_keys*/) const
{
    chassert(variant_result.aggregatorType() == AggregatorType::Memory);
    auto & result = static_cast<MemoryAggregatedDataVariants &>(variant_result);

    std::pair<bool, bool> return_result = {false, false};
    auto & need_abort = return_result.first;
    auto & need_finalization = return_result.second;

    /// `result will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    /// How to perform the aggregation?
    if (result.empty())
    {
        /// proton: starts. First init the key_sizes, and then the method since method init
        /// may depend on the key_sizes
        initDataVariants(result);
        initStatesForWithoutKey(result);
        /// proton: ends
        LOG_TRACE(logger, "Aggregation method: {}", result.getMethodName());
    }

    /** Constant columns are not supported directly during aggregation.
      * To make them work anyway, we materialize them.
      */
    Columns materialized_columns = materializeKeyColumns(columns, key_columns, result.isLowCardinality());

    /// proton: starts. For window start/end aggregation, we will need setup timestamp of the aggr to
    /// enable memory recycling.
    setupAggregatesPoolTimestamps(row_begin, row_end, key_columns, result.aggregates_pool);
    /// proton: ends

    NestedColumnsHolder nested_columns_holder;
    AggregateFunctionInstructions aggregate_functions_instructions;
    prepareAggregateInstructions(columns, aggregate_columns, materialized_columns, aggregate_functions_instructions, nested_columns_holder);

    /// We select one of the aggregation methods and call it.

    /// For the case when there are no keys (all aggregate into one row).
    if (result.type == MemoryAggregatedDataVariants::Type::without_key)
        need_finalization = executeWithoutKeyImpl(
            result.without_key, row_begin, row_end, aggregate_functions_instructions.data(), result.aggregates_pool);
    else
        need_finalization = executeImpl(result, row_begin, row_end, key_columns, aggregate_functions_instructions.data());

    need_abort = checkAndProcessResult(result);
    return return_result;
}

[[nodiscard]] bool MemoryAggregator::executeImpl(
    MemoryAggregatedDataVariants & result,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions) const
{
#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        return executeImplBatch(*result.NAME, result, row_begin, row_end, key_columns, aggregate_instructions); \
    }

    if (false)
    {
    } // NOLINT
    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M

    return false;
}

/** It's interesting - if you remove `noinline`, then gcc for some reason will inline this function, and the performance decreases (~ 10%).
  * (Probably because after the inline of this function, more internal functions no longer be inlined.)
  * Inline does not make sense, since the inner loop is entirely inside this function.
  */
template <typename Method>
[[nodiscard]] bool NO_INLINE MemoryAggregator::executeImplBatch(
    Method & method,
    MemoryAggregatedDataVariants & result,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    std::vector<Arena *> bucket_arenas;
    bool is_time_bucket_two_level = result.isTimeBucketTwoLevel();
    if (is_time_bucket_two_level)
    {
        bucket_arenas.resize(row_end - row_begin);

        for (size_t i = row_begin; i < row_end; ++i)
            bucket_arenas[i - row_begin] = result.getBucketAndArena(key_columns, i);
    }
    else
    {
        bucket_arenas.reserve(1);
        bucket_arenas.push_back(result.aggregates_pool);
    }

    /// Optimization for special case when there are no aggregate functions.
    if (params->aggregates_size == 0 && !needTrackUpdates())
    {
        /// For all rows.
        if (is_time_bucket_two_level)
        {
            for (size_t i = row_begin; i < row_end; ++i)
            {
                auto * target_arena = bucket_arenas[i - row_begin];
                state.emplaceKey(method.data, i, *target_arena).setMapped(target_arena->alloc(0));
            }
        }
        else
        {
            AggregateDataPtr place = result.aggregates_pool->alloc(0);
            for (size_t i = row_begin; i < row_end; ++i)
                state.emplaceKey(method.data, i, *result.aggregates_pool).setMapped(place);
        }
        return false;
    }

    bool need_finalization = false;

    /// Optimization for special case when aggregating by 8bit key.
    if constexpr (std::is_same_v<Method, typename decltype(MemoryAggregatedDataVariants::key8)::element_type>)
    {
        /// We use another method if there are aggregate functions with -Array combinator.
        bool has_arrays = false;
        for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
        {
            if (inst->offsets)
            {
                has_arrays = true;
                break;
            }
        }

        chassert(!is_time_bucket_two_level);

        if (!has_arrays && !needTrackUpdates())
        {
            auto * aggregates_pool = result.aggregates_pool;
            for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
            {
                inst->batch_that->addBatchLookupTable8(
                    row_begin,
                    row_end,
                    reinterpret_cast<AggregateDataPtr *>(method.data.data()),
                    inst->state_offset,
                    [&](AggregateDataPtr & aggregate_data) {
                        auto * data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                        createAggregateStates(data);
                        aggregate_data = data;
                    },
                    state.getKeyData(),
                    inst->batch_arguments,
                    aggregates_pool,
                    inst->delta_column);

                /// Calculate if we need finalization
                if (!need_finalization && inst->batch_that->isUserDefined())
                {
                    auto * map = reinterpret_cast<AggregateDataPtr *>(method.data.data());
                    const auto * key = state.getKeyData();
                    for (size_t cur = row_begin; !need_finalization && cur < row_end; ++cur)
                        need_finalization = (inst->batch_that->getEmitTimes(map[key[cur]] + inst->state_offset) > 0);
                }
            }
            return need_finalization;
        }
    }

#if USE_EMBEDDED_COMPILER
    auto use_compiled_functions = compiled_aggregate_functions_holder && !hasSparseArguments(aggregate_instructions);
#endif

    /// NOTE: only row_end-row_start is required, but:
    /// - this affects only optimize_aggregation_in_order,
    /// - this is just a pointer, so it should not be significant,
    /// - and plus this will require other changes in the interface.
    std::unique_ptr<AggregateDataPtr[]> places(new AggregateDataPtr[row_end]);

    /// For all rows.
    Arena * aggregates_pool = result.aggregates_pool;

    for (size_t i = row_begin; i < row_end; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        if (is_time_bucket_two_level)
            aggregates_pool = bucket_arenas[i - row_begin];

        auto emplace_result = state.emplaceKey(method.data, i, *aggregates_pool); /// FIXME, use window_bucket to emplace

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.isInserted())
        {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.setMapped(nullptr);

            aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
#if USE_EMBEDDED_COMPILER
            if (use_compiled_functions)
            {
                const auto & compiled_aggregate_functions = compiled_aggregate_functions_holder->compiled_aggregate_functions;
                compiled_aggregate_functions.create_aggregate_states_function(aggregate_data);
                /// Call to initialize reserved TrackingUpdates even all functions are compiled
                createAggregateStates(aggregate_data, /*skip_compiled_aggregate_functions=*/true);
            }
            else
#endif
            {
                createAggregateStates(aggregate_data);
            }

            emplace_result.setMapped(aggregate_data);
        }
        else
        {
            aggregate_data = emplace_result.getMapped();
        }

        chassert(aggregate_data != nullptr);
        places[i] = aggregate_data;
    }

#if USE_EMBEDDED_COMPILER
    /// add_into_aggregate_states_function does not support delta column
    if (use_compiled_functions && params->delta_col_pos < 0)
    {
        std::vector<ColumnData> columns_data;

        for (size_t i = 0; i < aggregate_functions.size(); ++i)
        {
            if (!is_aggregate_function_compiled[i])
                continue;

            AggregateFunctionInstruction * inst = aggregate_instructions + i;
            size_t arguments_size = inst->that->getArgumentTypes().size(); // NOLINT

            for (size_t argument_index = 0; argument_index < arguments_size; ++argument_index)
                columns_data.emplace_back(getColumnData(inst->batch_arguments[argument_index]));
        }

        auto add_into_aggregate_states_function
            = compiled_aggregate_functions_holder->compiled_aggregate_functions.add_into_aggregate_states_function;
        add_into_aggregate_states_function(row_begin, row_end, columns_data.data(), places.get());
    }
#endif

    /// Add values to the aggregate functions.
    for (size_t i = 0; i < aggregate_functions.size(); ++i)
    {
#if USE_EMBEDDED_COMPILER
        if (use_compiled_functions && params->delta_col_pos < 0 && is_aggregate_function_compiled[i])
            continue;
#endif

        AggregateFunctionInstruction * inst = aggregate_instructions + i;

        if (inst->offsets)
            inst->batch_that->addBatchArray(
                row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, inst->offsets, bucket_arenas);
        else
            inst->batch_that->addBatch(
                row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, bucket_arenas, -1, inst->delta_column);

        if (inst->batch_that->isUserDefined())
        {
            AggregateDataPtr * places_ptr = places.get();
            /// It is ok to re-flush if it is flush already, then we don't need maintain a map to check if it is ready flushed
            for (size_t j = row_begin; j < row_end; ++j)
            {
                if (places_ptr[j])
                {
                    inst->batch_that->flush(places_ptr[j] + inst->state_offset);
                    if (!need_finalization)
                        need_finalization = (inst->batch_that->getEmitTimes(places_ptr[j] + inst->state_offset) > 0);
                }
            }
        }
    }

    if (params->tracking_updates_type == TrackingUpdatesType::Updates)
        TrackingUpdates::addBatch(
            row_begin, row_end, places.get(), aggregate_instructions ? aggregate_instructions->delta_column : nullptr);
    else if (params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract)
        TrackingUpdatesWithRetract::addBatch(
            row_begin, row_end, places.get(), aggregate_instructions ? aggregate_instructions->delta_column : nullptr);

    return need_finalization;
}

}

}
