#include <Interpreters/Streaming/Aggregator/IAggregator.h>

#include <AggregateFunctions/AggregateFunctionArray.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/CompiledAggregateFunctionsHolder.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Common/SipHash.h>

namespace DB::Streaming
{
IAggregator::IAggregator(IAggregatorParamsPtr params_, const Block & input_header_, const std::string & aggregator)
    : params(std::move(params_)), input_header(input_header_), logger(getLogger(aggregator))
{
    calculateKeysPositions();
}

ColumnNumbers IAggregator::calculateKeysPositions()
{
    keys_positions.reserve(params->keys.size());
    for (const auto & key : params->keys)
        keys_positions.push_back(input_header.getPositionByName(key));
    return keys_positions;
}

void IAggregator::prepareAggregateInstructions(
    const Columns & columns,
    AggregateColumns & aggregate_columns,
    Columns & materialized_columns,
    AggregateFunctionInstructions & aggregate_functions_instructions,
    NestedColumnsHolder & nested_columns_holder) const
{
    for (size_t i = 0; i < params->aggregates_size; ++i)
        aggregate_columns[i].resize(params->aggregates[i].argument_names.size());

    aggregate_functions_instructions.resize(params->aggregates_size + 1);
    aggregate_functions_instructions[params->aggregates_size].that = nullptr;

    for (size_t i = 0; i < params->aggregates_size; ++i)
    {
        /// FIXME: Supports handle sparse columns
        bool allow_sparse_arguments = false; /// aggregate_columns[i].size() == 1;
        bool has_sparse_arguments = false;

        for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
        {
            const auto pos = input_header.getPositionByName(params->aggregates[i].argument_names[j]);
            materialized_columns.push_back(columns.at(pos)->convertToFullColumnIfConst());
            aggregate_columns[i][j] = materialized_columns.back().get();

            /// Sparse columns without defaults may be handled incorrectly.
            if (aggregate_columns[i][j]->isSparse() && aggregate_columns[i][j]->getNumberOfDefaultRows() == 0)
                allow_sparse_arguments = false;

            auto full_column
                = allow_sparse_arguments ? aggregate_columns[i][j]->getPtr() : recursiveRemoveSparse(aggregate_columns[i][j]->getPtr());

            full_column = recursiveRemoveLowCardinality(full_column);
            if (full_column.get() != aggregate_columns[i][j])
            {
                materialized_columns.emplace_back(std::move(full_column));
                aggregate_columns[i][j] = materialized_columns.back().get();
            }

            if (aggregate_columns[i][j]->isSparse())
                has_sparse_arguments = true;
        }

        aggregate_functions_instructions[i].has_sparse_arguments = has_sparse_arguments;
        aggregate_functions_instructions[i].arguments = aggregate_columns[i].data();
        aggregate_functions_instructions[i].state_offset = offsets_of_aggregate_states[i];

        const auto * that = aggregate_functions[i];
        /// Unnest consecutive trailing -State combinators
        while (const auto * func = typeid_cast<const AggregateFunctionState *>(that))
            that = func->getNestedFunction().get();
        aggregate_functions_instructions[i].that = that;

        if (const auto * func = typeid_cast<const AggregateFunctionArray *>(that))
        {
            /// Unnest consecutive -State combinators before -Array
            that = func->getNestedFunction().get();
            while (const auto * nested_func = typeid_cast<const AggregateFunctionState *>(that))
                that = nested_func->getNestedFunction().get();
            auto [nested_columns, offsets] = checkAndGetNestedArrayOffset(aggregate_columns[i].data(), that->getArgumentTypes().size());
            nested_columns_holder.push_back(std::move(nested_columns));
            aggregate_functions_instructions[i].batch_arguments = nested_columns_holder.back().data();
            aggregate_functions_instructions[i].offsets = offsets;
        }
        else
        {
            aggregate_functions_instructions[i].batch_arguments = aggregate_columns[i].data();
        }

        aggregate_functions_instructions[i].batch_that = that;

        if (params->delta_col_pos >= 0)
            aggregate_functions_instructions[i].delta_column = columns.at(params->delta_col_pos).get(); /// point to the column point is ok
    }
}

Columns IAggregator::materializeKeyColumns(const Columns & columns, ColumnRawPtrs & key_columns, bool is_low_cardinality) const
{
    Columns materialized_columns;
    materialized_columns.reserve(params->keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params->keys_size; ++i)
    {
        materialized_columns.push_back(columns.at(keys_positions[i])->convertToFullColumnIfConst());
        key_columns[i] = materialized_columns.back().get();

        if (!is_low_cardinality)
        {
            auto column_no_lc = recursiveRemoveLowCardinality(key_columns[i]->getPtr());
            if (column_no_lc.get() != key_columns[i])
            {
                materialized_columns.back().swap(column_no_lc);
                key_columns[i] = materialized_columns.back().get();
            }
        }
    }

    return materialized_columns;
}

IAggregator::OutputBlockColumns
IAggregator::prepareOutputBlockColumns(const Block & res_header, const Arenas & aggregates_pools, bool final, size_t rows) const
{
    auto aggregates_size = params->aggregates.size();
    MutableColumns key_columns(params->keys_size);
    MutableColumns aggregate_columns(aggregates_size);
    MutableColumns final_aggregate_columns(aggregates_size);
    AggregateColumnsData aggregate_columns_data(aggregates_size);

    for (size_t i = 0; i < params->keys_size; ++i)
    {
        key_columns[i] = res_header.safeGetByPosition(i).type->createColumn();
        key_columns[i]->reserve(rows);
    }

    for (size_t i = 0; i < aggregates_size; ++i)
    {
        if (!final)
        {
            const auto & aggregate_column_name = params->aggregates[i].column_name;
            aggregate_columns[i] = res_header.getByName(aggregate_column_name).type->createColumn();

            /// The ColumnAggregateFunction column captures the shared ownership of the arena with the aggregate function states.
            ColumnAggregateFunction & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);

            /// Streaming aggregating always keep aggregated state.
            column_aggregate_func.setKeepState(true);

            /// Add arenas to ColumnAggregateFunction, which can result in moving ownership to it if reference count
            /// get dropped in other places
            for (const auto & pool : aggregates_pools)
                column_aggregate_func.addArena(pool);

            aggregate_columns_data[i] = &column_aggregate_func.getData();
            aggregate_columns_data[i]->reserve(rows);
        }
        else
        {
            final_aggregate_columns[i] = aggregate_functions[i]->getResultType()->createColumn();
            final_aggregate_columns[i]->reserve(rows);

            if (aggregate_functions[i]->isState())
            {
                auto callback = [&](IColumn & column) {
                    /// The ColumnAggregateFunction column captures the shared ownership of the arena with aggregate function states.
                    if (auto * column_aggregate_func = typeid_cast<ColumnAggregateFunction *>(&column))
                        for (const auto & pool : aggregates_pools)
                            column_aggregate_func->addArena(pool);
                };

                callback(*final_aggregate_columns[i]);
                final_aggregate_columns[i]->forEachMutableSubcolumnRecursively(callback);
            }
        }
    }

    std::vector<IColumn *> raw_key_columns;
    raw_key_columns.reserve(key_columns.size());
    for (auto & column : key_columns)
        raw_key_columns.push_back(column.get());

    return {
        .key_columns = std::move(key_columns),
        .raw_key_columns = std::move(raw_key_columns),
        .aggregate_columns = std::move(aggregate_columns),
        .final_aggregate_columns = std::move(final_aggregate_columns),
        .aggregate_columns_data = std::move(aggregate_columns_data),
    };
}

Block IAggregator::finalizeBlock(const Block & res_header, OutputBlockColumns && out_cols, bool final, size_t rows) const
{
    auto && [key_columns, raw_key_columns, aggregate_columns, final_aggregate_columns, aggregate_columns_data] = out_cols;

    Block res = res_header.cloneEmpty();

    auto aggregates_size = params->aggregates.size();
    for (size_t i = 0; i < params->keys_size; ++i)
        res.getByPosition(i).column = std::move(key_columns[i]);

    for (size_t i = 0; i < aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params->aggregates[i].column_name;
        if (final)
            res.getByName(aggregate_column_name).column = std::move(final_aggregate_columns[i]);
        else
            res.getByName(aggregate_column_name).column = std::move(aggregate_columns[i]);
    }

    /// Change the size of the columns-constants in the block.
    size_t columns = res_header.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        if (isColumnConst(*res.getByPosition(i).column))
            res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);
    }

    return res;
}

/// Merge aggregate states from src to dst
ALWAYS_INLINE void IAggregator::mergeAggregateStates(
    AggregateDataPtr dst, ConstAggregateDataPtr src, Arena * arena, bool skip_compiled_aggregate_functions) const
{
    chassert(src);
    chassert(dst);

    for (size_t i = 0; i < params->aggregates_size; ++i)
    {
        if (skip_compiled_aggregate_functions && is_aggregate_function_compiled[i])
            continue;
        aggregate_functions[i]->merge(dst + offsets_of_aggregate_states[i], src + offsets_of_aggregate_states[i], arena);
    }
}

ALWAYS_INLINE void IAggregator::doDestroyAggregateStates(AggregateDataPtr place) const
{
    if (!all_aggregates_has_trivial_destructor && place)
    {
        for (size_t j = 0; j < params->aggregates_size; ++j)
            aggregate_functions[j]->destroy(place + offsets_of_aggregate_states[j]);
    }
}

bool IAggregator::hasSparseArguments(AggregateFunctionInstruction * aggregate_instructions)
{
    for (auto * inst = aggregate_instructions; inst->that != nullptr; ++inst)
        if (inst->has_sparse_arguments)
            return true;
    return false;
}

#if USE_EMBEDDED_COMPILER
void IAggregator::compileAggregateFunctionsIfNeeded()
{
    static std::unordered_map<UInt128, UInt64, UInt128Hash> aggregate_functions_description_to_count;
    static std::mutex mutex;

    if (!params->compile_aggregate_expressions)
        return;

    std::vector<AggregateFunctionWithOffset> functions_to_compile;
    String functions_description;

    is_aggregate_function_compiled.resize(aggregate_functions.size());

    /// Add values to the aggregate functions.
    for (size_t i = 0; i < aggregate_functions.size(); ++i)
    {
        const auto * function = aggregate_functions[i];
        size_t offset_of_aggregate_function = offsets_of_aggregate_states[i];

        if (function->isCompilable())
        {
            AggregateFunctionWithOffset function_to_compile{.function = function, .aggregate_data_offset = offset_of_aggregate_function};

            functions_to_compile.emplace_back(std::move(function_to_compile));

            functions_description += function->getDescription();
            functions_description += ' ';

            functions_description += std::to_string(offset_of_aggregate_function);
            functions_description += ' ';
        }

        is_aggregate_function_compiled[i] = function->isCompilable();
    }

    if (functions_to_compile.empty())
        return;

    SipHash aggregate_functions_description_hash;
    aggregate_functions_description_hash.update(functions_description);

    UInt128 aggregate_functions_description_hash_key;
    aggregate_functions_description_hash.get128(aggregate_functions_description_hash_key);

    {
        std::lock_guard<std::mutex> lock(mutex);

        if (aggregate_functions_description_to_count[aggregate_functions_description_hash_key]++
            < params->min_count_to_compile_aggregate_expression)
            return;

        if (auto * compilation_cache = CompiledExpressionCacheFactory::instance().tryGetCache())
        {
            auto [compiled_function_cache_entry, _] = compilation_cache->getOrSet(aggregate_functions_description_hash_key, [&]() {
                LOG_TRACE(logger, "Compile expression {}", functions_description);

                auto compiled_aggregate_functions
                    = compileAggregateFunctions(getJITInstance(), functions_to_compile, functions_description);
                return std::make_shared<CompiledAggregateFunctionsHolder>(std::move(compiled_aggregate_functions));
            });

            compiled_aggregate_functions_holder = std::static_pointer_cast<CompiledAggregateFunctionsHolder>(compiled_function_cache_entry);
        }
        else
        {
            LOG_TRACE(logger, "Compile expression {}", functions_description);
            auto compiled_aggregate_functions = compileAggregateFunctions(getJITInstance(), functions_to_compile, functions_description);
            compiled_aggregate_functions_holder
                = std::make_shared<CompiledAggregateFunctionsHolder>(std::move(compiled_aggregate_functions));
        }
    }
}
#endif

}
