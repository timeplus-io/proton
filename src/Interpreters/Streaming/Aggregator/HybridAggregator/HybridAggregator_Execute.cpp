#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingCount.h>

#include <Interpreters/CompiledAggregateFunctionsHolder.h>
#include <Common/HybridHashTable/HybridKeyGetter.h>

namespace DB::Streaming
{

std::pair<bool, bool> HybridAggregator::executeAndRetractOnBlock(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & variants_result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    bool new_keys) const
{
    return doExecuteOnBlock(
        std::move(columns), row_begin, row_end, variants_result, key_columns, aggregate_columns, new_keys, /*tracking_retracts=*/true);
}

/// \return {should_abort, need_finalization}
std::pair<bool, bool> HybridAggregator::executeOnBlock(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & variants_result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    bool new_keys) const
{
    return doExecuteOnBlock(
        std::move(columns), row_begin, row_end, variants_result, key_columns, aggregate_columns, new_keys, /*tracking_retracts=*/false);
}

std::pair<bool, bool> HybridAggregator::doExecuteOnBlock(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & variants_result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    bool new_keys,
    bool tracking_retracts) const
{
    chassert(variants_result.aggregatorType() == AggregatorType::Hybrid);
    auto & result = static_cast<HybridAggregatedDataVariants &>(variants_result);
    std::pair<bool, bool> return_result = {false, false};
    auto & need_finalization = return_result.second;

    result.aggregator = this;
    if (result.empty())
        initStates(result);

    /// Constant columns are not supported directly during aggregation.
    /// To make them work anyway, we materialize them.
    Columns materialized_columns = materializeKeyColumns(columns, key_columns, result.isLowCardinality());

    NestedColumnsHolder nested_columns_holder;
    AggregateFunctionInstructions aggregate_functions_instructions;
    prepareAggregateInstructions(columns, aggregate_columns, materialized_columns, aggregate_functions_instructions, nested_columns_holder);

    switch (method_chosen)
    {
        case HybridHashType::Empty:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash table is not inited");
        }
        case HybridHashType::key_hashed:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Hashed key shall not be chosen for aggregation");
        }
        case HybridHashType::WithoutKey:
        {
            need_finalization = executeWithoutKeyImpl(result, row_begin, row_end, aggregate_functions_instructions.data());
            break;
        }

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        if (has_nullable_key) \
        { \
            HybridKeyGetter<HybridHashType::NAME, /*nullable=*/true> key_getter{key_columns, key_sizes}; \
            need_finalization = executeOnBlockImpl( \
                *result.table.NAME, \
                result.updates.NAME.get(), \
                tracking_retracts ? result.retracts.NAME.get() : nullptr, \
                key_getter, \
                row_begin, \
                row_end, \
                aggregate_functions_instructions.data(), \
                new_keys); \
        } \
        else \
        { \
            HybridKeyGetter<HybridHashType::NAME, /*nullable=*/false> key_getter{key_columns, key_sizes}; \
            need_finalization = executeOnBlockImpl( \
                *result.table.NAME, \
                result.updates.NAME.get(), \
                tracking_retracts ? result.retracts.NAME.get() : nullptr, \
                key_getter, \
                row_begin, \
                row_end, \
                aggregate_functions_instructions.data(), \
                new_keys); \
        } \
        break; \
    }
            APPLY_FOR_HASH_KEY_VARIANTS_HYBRID(M)
#undef M
    }

    return return_result;
}

/// It's interesting - if you remove `noinline`, then gcc for some reason will inline this function, and the performance decreases (~ 10%).
/// (Probably because after the inline of this function, more internal functions no longer be inlined.)
/// Inline does not make sense, since the inner loop is entirely inside this function.
template <typename Table, typename KeyGetter>
[[nodiscard]] bool NO_INLINE HybridAggregator::executeOnBlockImpl(
    Table & table,
    Table * updates,
    Table * retracts,
    const KeyGetter & key_getter,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    bool new_keys) const
{
#if USE_EMBEDDED_COMPILER
    auto use_compiled_functions = compiled_aggregate_functions_holder && !hasSparseArguments(aggregate_instructions);
#endif

    /// NOTE: only row_end-row_start is required, but:
    /// - this affects only optimize_aggregation_in_order,
    /// - this is just a pointer, so it should not be significant,
    /// - and plus this will require other changes in the interface.
    std::unique_ptr<AggregateDataPtr[]> places;

    auto rows = row_end - row_begin;
    {
        std::vector<typename KeyGetter::KeyType> keys;
        keys.reserve(rows);

        for (size_t row = row_begin; row < row_end; ++row)
            keys.emplace_back(key_getter.getKeyHolder(row));

        auto emplace_results = new_keys ? table.emplaceNewKeys(keys) : table.emplaceKeys(keys);
        if (emplace_results.hasError())
            throw Exception::createRuntime(emplace_results.errorCode(), emplace_results.errorString());

        assert(emplace_results.results.size() == rows);

        places.reset(new AggregateDataPtr[row_end]);

        for (size_t row = row_begin; auto & emplace_result : emplace_results.results)
            places[row++] = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());

        if (updates)
        {
            auto updates_emplace_results = new_keys ? updates->emplaceNewKeys(keys) : updates->emplaceKeys(keys);
            if (updates_emplace_results.hasError())
                throw Exception::createRuntime(updates_emplace_results.errorCode(), updates_emplace_results.errorString());

            assert(updates_emplace_results.results.size() == rows);
        }

        if (retracts)
        {
            auto retracts_emplace_results = new_keys ? retracts->emplaceNewKeys(keys) : retracts->emplaceKeys(keys);
            if (retracts_emplace_results.hasError())
                throw Exception::createRuntime(retracts_emplace_results.errorCode(), retracts_emplace_results.errorString());

            chassert(retracts_emplace_results.results.size() == rows);
            for (size_t row = row_begin; auto & retracts_emplace_result : retracts_emplace_results.results)
            {
                auto * src_place = static_cast<ConstAggregateDataPtr>(places[row++]);
                /// retracts doesn't have the saved aggregation states yet
                /// Save the current aggregate states before mutating them below
                if (retracts_emplace_result.isInserted() && !TrackingCount::empty(src_place + tracking_count_offset))
                {
                    auto * dst_place = static_cast<AggregateDataPtr>(retracts_emplace_result.getMutableMapped());
                    mergeAggregateStates(dst_place, src_place, /*arena=*/nullptr);
                    TrackingCount::merge(dst_place + tracking_count_offset, src_place + tracking_count_offset);
                }
            }
        }
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
    for (size_t i = 0, func_size = aggregate_functions.size(); i < func_size; ++i)
    {
#if USE_EMBEDDED_COMPILER
        if (use_compiled_functions && params->delta_col_pos < 0 && is_aggregate_function_compiled[i])
            continue;
#endif

        AggregateFunctionInstruction * inst = aggregate_instructions + i;

        if (inst->offsets)
            inst->batch_that->addBatchArray(
                row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, inst->offsets, /*arenas=*/{nullptr});
        else
            inst->batch_that->addBatch(
                row_begin,
                row_end,
                places.get(),
                inst->state_offset,
                inst->batch_arguments,
                /*arenas=*/{nullptr},
                -1,
                inst->delta_column); /// FIXME arenas

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

    if (trackingStateCount())
        TrackingCount::addBatch(
            row_begin,
            row_end,
            places.get(),
            tracking_count_offset,
            aggregate_instructions ? aggregate_instructions->delta_column : nullptr);

    table.spillIfNecessary();

    return need_finalization;
}

}
