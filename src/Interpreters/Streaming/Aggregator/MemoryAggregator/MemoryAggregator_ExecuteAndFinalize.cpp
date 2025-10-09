#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED;
}

namespace Streaming
{
Block MemoryAggregator::executeAndFinalizeWithoutKeyPerRowImpl(
    AggregateDataPtr aggregate_data,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    Arena * arena,
    Arenas & aggregates_pools) const
{
    constexpr bool final = true;
    OutputBlockColumns out_cols = prepareOutputBlockColumns(getHeader(final), aggregates_pools, final, row_end - row_begin + 1);
    for (size_t i = row_begin; i < row_end; ++i)
        addAndInsertAggregatesIntoColumns(aggregate_data, aggregate_instructions, out_cols.final_aggregate_columns, i, arena);

    return finalizeBlock(getHeader(final), std::move(out_cols), final, row_end - row_begin);
}

template <typename Method>
Block MemoryAggregator::executeAndFinalizePerRowImpl(
    Method & method,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    Arena * arena,
    Arenas & aggregates_pools) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    constexpr bool final = true;
    OutputBlockColumns out_cols = prepareOutputBlockColumns(getHeader(final), aggregates_pools, final, row_end - row_begin + 1);
    auto shuffled_key_sizes = method.shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

    AggregateDataPtr aggregate_data = nullptr;
    for (size_t i = row_begin; i < row_end; ++i)
    {
        auto emplace_result = state.emplaceKey(method.data, i, *arena);

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.isInserted())
        {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.setMapped(nullptr);
            aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data);
            emplace_result.setMapped(aggregate_data);
        }
        else
        {
            aggregate_data = emplace_result.getMapped();
        }

        chassert(aggregate_data != nullptr);

        /// Insert the key into the output columns.
        auto key_holder = state.getKeyHolder(i, *arena);
        method.insertKeyIntoColumns(keyHolderGetKey(key_holder), out_cols.raw_key_columns, key_sizes_ref);
        keyHolderDiscardKey(key_holder);

        /// Add and insert aggregates into columns.
        addAndInsertAggregatesIntoColumns(aggregate_data, aggregate_instructions, out_cols.final_aggregate_columns, i, arena);
    }

    return finalizeBlock(getHeader(final), std::move(out_cols), final, row_end - row_begin);
}

Block MemoryAggregator::executeAndFinalizePerRow(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & variants_result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    bool new_keys) const
{
    if (unlikely(row_end <= row_begin))
        return {};

    chassert(variants_result.aggregatorType() == AggregatorType::Memory);
    auto & result = static_cast<MemoryAggregatedDataVariants &>(variants_result);

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

    switch (result.type)
    {
        case MemoryAggregatedDataVariants::Type::without_key:
        {
            return executeAndFinalizeWithoutKeyPerRowImpl(
                result.without_key,
                row_begin,
                row_end,
                aggregate_functions_instructions.data(),
                result.aggregates_pool,
                result.aggregates_pools);
        }
#define M(NAME, IS_TWO_LEVEL) \
        case MemoryAggregatedDataVariants::Type::NAME: \
        { \
            return executeAndFinalizePerRowImpl( \
                *result.NAME, \
                row_begin, \
                row_end, \
                key_columns, \
                aggregate_functions_instructions.data(), \
                result.aggregates_pool, \
                result.aggregates_pools); \
        }

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
        default:
            throw Exception(ErrorCodes::UNSUPPORTED, "Unknown MemoryAggregatedDataVariants type: {}", result.type);
    }
}

Block MemoryAggregator::executeAndFinalizeAfterKeyExpire(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
    bool new_keys) const
{
    (void)columns;
    (void)row_begin;
    (void)row_end;
    (void)result;
    (void)key_columns;
    (void)aggregate_columns;
    (void)new_keys;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "`EMIT AFTER KEY EXPIRE` is not supported by memory aggregator yet");
}

}

}
