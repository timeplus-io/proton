#include <Interpreters/Streaming/Aggregator/IAggregator.h>

#include <Interpreters/CompiledAggregateFunctionsHolder.h>

namespace DB::Streaming
{

ALWAYS_INLINE void
IAggregator::insertAggregatesIntoColumns(AggregateDataPtr mapped, const MutableColumns & final_aggregate_columns, Arena * arena) const
{
    /// Insert final values of aggregate functions into columns.
    for (size_t i = 0; i < params->aggregates_size; ++i)
        aggregate_functions[i]->insertResultInto(
            mapped + offsets_of_aggregate_states[i], *final_aggregate_columns[i], arena); /// FIXME arena
}

ALWAYS_INLINE void IAggregator::addAndInsertAggregatesIntoColumns(
    AggregateDataPtr aggregate_data,
    AggregateFunctionInstruction * aggregate_instructions,
    const MutableColumns & final_aggregate_columns,
    size_t row_num,
    Arena * arena) const
{
    for (size_t i = 0; i < params->aggregates_size; ++i)
    {
        auto * inst = aggregate_instructions + i;
        auto place = aggregate_data + inst->state_offset;
        if (!aggregate_instructions->delta_column
            || assert_cast<const ColumnInt8 &>(*aggregate_instructions->delta_column).getData()[row_num] > 0)
            inst->batch_that->add(place, inst->batch_arguments, row_num, arena);
        else
            inst->batch_that->negate(place, inst->batch_arguments, row_num, arena);

        if (inst->batch_that->isUserDefined())
            inst->batch_that->flush(place);

        inst->batch_that->insertResultInto(place, *final_aggregate_columns[i], arena);
    }
}

ALWAYS_INLINE Block IAggregator::insertResultsIntoColumns(
    PaddedPODArray<AggregateDataPtr> & places,
    OutputBlockColumns && out_cols,
    Arena * arena,
    bool use_compiled_functions [[maybe_unused]]) const
{
#if USE_EMBEDDED_COMPILER
    if (use_compiled_functions)
    {
        /** For JIT compiled functions we need to resize columns before pass them into compiled code.
            * insert_aggregates_into_columns_function function does not throw exception.
            */
        std::vector<ColumnData> columns_data;

        auto compiled_functions = compiled_aggregate_functions_holder->compiled_aggregate_functions;

        for (size_t i = 0; i < params->aggregates_size; ++i)
        {
            if (!is_aggregate_function_compiled[i])
                continue;

            auto & final_aggregate_column = out_cols.final_aggregate_columns[i];
            final_aggregate_column = final_aggregate_column->cloneResized(places.size());
            columns_data.emplace_back(getColumnData(final_aggregate_column.get()));
        }

        auto insert_aggregates_into_columns_function = compiled_functions.insert_aggregates_into_columns_function;
        insert_aggregates_into_columns_function(0, places.size(), columns_data.data(), places.data());
    }
#endif

    for (size_t i = 0; i < params->aggregates_size; ++i)
    {
#if USE_EMBEDDED_COMPILER
        if (use_compiled_functions && is_aggregate_function_compiled[i])
            continue;
#endif

        auto & final_aggregate_column = out_cols.final_aggregate_columns[i];
        size_t offset = offsets_of_aggregate_states[i];

        aggregate_functions[i]->insertResultIntoBatch(
            0,
            places.size(),
            places.data(),
            offset,
            *final_aggregate_column,
            arena, /// FIXME arena
            /*destroy_place_after_insert=*/false);
    }

    return finalizeBlock(getHeader(/*final_=*/true), std::move(out_cols), /*final=*/true, places.size());
}

ALWAYS_INLINE Block
IAggregator::insertResultsIntoColumns(PaddedPODArray<ConstAggregateDataPtr> & places, OutputBlockColumns && out_cols, Arena * arena) const
{
    for (size_t i = 0; i < params->aggregates_size; ++i)
    {
        auto & final_aggregate_column = out_cols.final_aggregate_columns[i];
        size_t offset = offsets_of_aggregate_states[i];

        /// NOTE: We should guarantee that calls to insertResultIntoBatch() are idempotent, meaning that multiple calls have the same result.
        aggregate_functions[i]->insertResultIntoBatch(
            0,
            places.size(),
            const_cast<AggregateDataPtr *>(places.data()),
            offset,
            *final_aggregate_column,
            arena, /// FIXME arena
            /*destroy_place_after_insert=*/false);
    }

    return finalizeBlock(getHeader(/*final_=*/true), std::move(out_cols), /*final=*/true, places.size());
}

}
