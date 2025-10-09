#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>

#include <Interpreters/CompiledAggregateFunctionsHolder.h>

namespace DB
{

namespace Streaming
{

std::pair<bool, bool> MemoryAggregator::executeAndRetractOnBlock(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & variants_result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    bool /*new_keys*/) const
{
    chassert(variants_result.aggregatorType() == AggregatorType::Memory);
    auto & result = static_cast<MemoryAggregatedDataVariants &>(variants_result);

    std::pair<bool, bool> return_result = {false, false};
    auto & need_abort = return_result.first;
    auto & need_finalization = return_result.second;

    if (unlikely(row_end <= row_begin))
        return return_result;

    result.aggregator = this;
    if (result.empty())
    {
        initDataVariants(result);
        initStatesForWithoutKey(result);
        LOG_TRACE(logger, "Aggregation method: {}", result.getMethodName());
    }

    Columns materialized_columns = materializeKeyColumns(columns, key_columns, result.isLowCardinality());

    setupAggregatesPoolTimestamps(row_begin, row_end, key_columns, result.aggregates_pool);

    NestedColumnsHolder nested_columns_holder;
    AggregateFunctionInstructions aggregate_functions_instructions;

    prepareAggregateInstructions(columns, aggregate_columns, materialized_columns, aggregate_functions_instructions, nested_columns_holder);

    chassert(params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract);
    if (result.type == MemoryAggregatedDataVariants::Type::without_key)
    {
        /// Save last finalization state into `retracted_result` before processing new data.
        /// We shall clear and reset it after finalization
        if (!TrackingUpdatesWithRetract::hasRetract(result.without_key))
        {
            auto & retract_data = TrackingUpdatesWithRetract::getRetract(result.without_key);
            auto * aggregate_data = result.retract_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data);
            retract_data = aggregate_data;
            if (!TrackingUpdatesWithRetract::empty(result.without_key))
            {
                mergeAggregateStates(retract_data, result.without_key, result.retract_pool.get());
                TrackingUpdatesWithRetract::merge(retract_data, result.without_key);
            }
        }

        need_finalization = executeWithoutKeyImpl(
            result.without_key, row_begin, row_end, aggregate_functions_instructions.data(), result.aggregates_pool);
    }

#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        need_finalization \
            = executeAndRetractImpl(*result.NAME, result, row_begin, row_end, key_columns, aggregate_functions_instructions.data()); \
    }

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M

    need_abort = checkAndProcessResult(result);
    return return_result;
}

template <typename Method>
bool MemoryAggregator::executeAndRetractImpl(
    Method & method,
    MemoryAggregatedDataVariants & result,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    auto * aggregates_pool = result.aggregates_pool;

#if USE_EMBEDDED_COMPILER
    auto use_compiled_functions = compiled_aggregate_functions_holder && !hasSparseArguments(aggregate_instructions);
#endif

    /// NOTE: only row_end-row_start is required, but:
    /// - this affects only optimize_aggregation_in_order,
    /// - this is just a pointer, so it should not be significant,
    /// - and plus this will require other changes in the interface.
    std::unique_ptr<AggregateDataPtr[]> places(new AggregateDataPtr[row_end]);

    /// For all rows.
    for (size_t i = row_begin; i < row_end; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplaceKey(method.data, i, *aggregates_pool);

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

        /// Save changed group with retracted state (used for emit changed group)
        /// If there are aggregate data and no retracted data, copy aggregate data to retracted data before changed
        if (!TrackingUpdatesWithRetract::hasRetract(aggregate_data))
        {
            auto & retract_data = TrackingUpdatesWithRetract::getRetract(aggregate_data);
            auto tmp_retract = result.retract_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(tmp_retract);
            retract_data = tmp_retract;
            if (!TrackingUpdatesWithRetract::empty(aggregate_data))
            {
                mergeAggregateStates(retract_data, aggregate_data, result.retract_pool.get());
                TrackingUpdatesWithRetract::merge(retract_data, aggregate_data);
            }
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

    bool need_finalization = false;
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
                row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, inst->offsets, {aggregates_pool});
        else
            inst->batch_that->addBatch(
                row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, {aggregates_pool}, -1, inst->delta_column);

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

    TrackingUpdatesWithRetract::addBatch(
        row_begin, row_end, places.get(), aggregate_instructions ? aggregate_instructions->delta_column : nullptr);

    return need_finalization;
}

}

}
