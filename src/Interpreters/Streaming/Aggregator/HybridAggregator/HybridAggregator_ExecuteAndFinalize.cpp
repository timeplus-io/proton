#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingTime.h>
#include <Common/HybridHashTable/HybridKeyGetter.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_MANY_TRACKING_KEYS;
}

namespace Streaming
{
Block HybridAggregator::executeAndFinalizeWithoutKeyPerRowImpl(
    AggregateDataPtr aggregate_data, size_t row_begin, size_t row_end, AggregateFunctionInstruction * aggregate_instructions) const
{
    constexpr bool final = true;
    OutputBlockColumns out_cols = prepareOutputBlockColumns(getHeader(final), /*aggregates_pools=*/{}, final, row_end - row_begin + 1);
    for (size_t i = row_begin; i < row_end; ++i)
        addAndInsertAggregatesIntoColumns(aggregate_data, aggregate_instructions, out_cols.final_aggregate_columns, i, /*arena=*/nullptr);

    return finalizeBlock(getHeader(final), std::move(out_cols), final, row_end - row_begin);
}

template <typename Table, typename KeyGetter>
Block NO_INLINE HybridAggregator::executeAndFinalizePerRowImpl(
    Table & table,
    const KeyGetter & key_getter,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    bool new_keys) const
{
    constexpr bool final = true;
    OutputBlockColumns out_cols = prepareOutputBlockColumns(getHeader(final), /*aggregates_pools=*/{}, final, row_end - row_begin + 1);
    auto shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

    HybridEmplaceResult emplace_result;
    AggregateDataPtr aggregate_data;
    for (size_t i = row_begin; i < row_end; ++i)
    {
        auto key = key_getter.getKeyHolder(i);
        emplace_result = new_keys ? table.emplaceNewKey(key) : table.emplaceKey(key, /*disable_spill=*/false);
        if (emplace_result.hasError())
            throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

        aggregate_data = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());
        chassert(aggregate_data != nullptr);

        /// Insert the key into the output columns.
        KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);

        /// Add and insert aggregates into columns.
        addAndInsertAggregatesIntoColumns(aggregate_data, aggregate_instructions, out_cols.final_aggregate_columns, i, /*arena=*/nullptr);
    }

    return finalizeBlock(getHeader(final), std::move(out_cols), final, row_end - row_begin);
}

Block HybridAggregator::executeAndFinalizePerRow(
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

    chassert(variants_result.aggregatorType() == AggregatorType::Hybrid);
    auto & result = static_cast<HybridAggregatedDataVariants &>(variants_result);

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
        case HybridHashType::WithoutKey:
        {
            return executeAndFinalizeWithoutKeyPerRowImpl(
                result.without_key.get(), row_begin, row_end, aggregate_functions_instructions.data());
        }

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        if (has_nullable_key) \
        { \
            HybridKeyGetter<HybridHashType::NAME, /*nullable=*/true> key_getter{key_columns, key_sizes}; \
            return executeAndFinalizePerRowImpl( \
                *result.table.NAME, key_getter, row_begin, row_end, aggregate_functions_instructions.data(), new_keys); \
        } \
        else \
        { \
            HybridKeyGetter<HybridHashType::NAME, /*nullable=*/false> key_getter{key_columns, key_sizes}; \
            return executeAndFinalizePerRowImpl( \
                *result.table.NAME, key_getter, row_begin, row_end, aggregate_functions_instructions.data(), new_keys); \
        } \
    }
            APPLY_FOR_HASH_KEY_VARIANTS_HYBRID(M)
#undef M
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} group by key is not supported by `EMIT PER EVENT`", method_chosen);
    }
}

Block HybridAggregator::executeAndFinalizeAfterKeyExpire(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & variants_result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    bool new_keys) const
{
    chassert(variants_result.aggregatorType() == AggregatorType::Hybrid);
    auto & result = static_cast<HybridAggregatedDataVariants &>(variants_result);

    result.aggregator = this;
    if (result.empty())
        initStates(result);

    chassert(params->emit_key_params->key_ts_col_pos < columns.size());

    const auto * ts_col = columns[params->emit_key_params->key_ts_col_pos].get();

    /// Constant columns are not supported directly during aggregation.
    /// To make them work anyway, we materialize them.
    Columns materialized_columns = materializeKeyColumns(columns, key_columns, result.isLowCardinality());

    NestedColumnsHolder nested_columns_holder;
    AggregateFunctionInstructions aggregate_functions_instructions;
    prepareAggregateInstructions(columns, aggregate_columns, materialized_columns, aggregate_functions_instructions, nested_columns_holder);

    switch (method_chosen)
    {
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} group by key is not supported by `EMIT AFTER KEY EXPIRE`", method_chosen);

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        if (has_nullable_key) \
        { \
            HybridKeyGetter<HybridHashType::NAME, /*nullable=*/true> key_getter{key_columns, key_sizes}; \
            return executeAndFinalizeAfterKeyExpireImpl( \
                *result.table.NAME, \
                *result.outstanding_keys.NAME, \
                ts_col, \
                key_getter, \
                row_begin, \
                row_end, \
                aggregate_functions_instructions.data(), \
                new_keys); \
        } \
        else \
        { \
            HybridKeyGetter<HybridHashType::NAME, /*nullable=*/false> key_getter{key_columns, key_sizes}; \
            return executeAndFinalizeAfterKeyExpireImpl( \
                *result.table.NAME, \
                *result.outstanding_keys.NAME, \
                ts_col, \
                key_getter, \
                row_begin, \
                row_end, \
                aggregate_functions_instructions.data(), \
                new_keys); \
        } \
        break; \
    }
            APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
#undef M
    }
}

/// It's interesting - if you remove `noinline`, then gcc for some reason will inline this function, and the performance decreases (~ 10%).
/// (Probably because after the inline of this function, more internal functions no longer be inlined.)
/// Inline does not make sense, since the inner loop is entirely inside this function.
template <typename Table, typename KeyList, typename KeyGetter>
[[nodiscard]] Block NO_INLINE HybridAggregator::executeAndFinalizeAfterKeyExpireImpl(
    Table & table,
    KeyList & outstanding_keys,
    const IColumn * ts_col,
    const KeyGetter & key_getter,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    bool new_keys) const
{
    /// NOTE: only row_end-row_start is required, but:
    /// - this affects only optimize_aggregation_in_order,
    /// - this is just a pointer, so it should not be significant,
    /// - and plus this will require other changes in the interface.
    std::unique_ptr<AggregateDataPtr[]> places;
    std::vector<typename KeyGetter::KeyType> keys;

    absl::flat_hash_set<typename KeyGetter::KeyType> expired_key_set;
    HybridEmplaceResults emplace_results;

    Block block;
    if (auto rows = row_end - row_begin; rows > 0)
    {
        keys.reserve(rows);

        for (size_t row = row_begin; row < row_end; ++row)
            keys.emplace_back(key_getter.getKeyHolder(row));

        emplace_results = new_keys ? table.emplaceNewKeys(keys) : table.emplaceKeys(keys);
        if (emplace_results.hasError())
            throw Exception::createRuntime(emplace_results.errorCode(), emplace_results.errorString());

        chassert(emplace_results.results.size() == rows);

        places.reset(new AggregateDataPtr[row_end]);

        for (size_t row = row_begin; auto & emplace_result : emplace_results.results)
            places[row++] = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());

        /// Add values to the aggregate functions.
        for (size_t i = 0, func_size = aggregate_functions.size(); i < func_size; ++i)
        {
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
                        inst->batch_that->flush(places_ptr[j] + inst->state_offset);
                }
            }
        }

        /// 1) Check expired keys of the current insert batch
        std::vector<typename KeyGetter::KeyType> expired_keys;
        PaddedPODArray<ConstAggregateDataPtr> expired_places;
        auto * places_ptr = places.get();

        for (size_t row = row_begin; row < row_end; ++row)
        {
            TrackingTime::updateTimestamp(places_ptr[row], ts_col->get64(row));

            if (TrackingTime::maxSpanReached(places_ptr[row], params->emit_key_params->key_max_span_interval))
            {
                expired_keys.push_back(std::move(keys[row - row_begin]));
                expired_places.emplace_back(places_ptr[row]);
            }
        }

        if (!expired_keys.empty())
        {
            OutputBlockColumns out_cols
                = prepareOutputBlockColumns(getHeader(/*final=*/true), /*aggregates_pools=*/{}, /*final=*/true, row_end - row_begin + 1);
            auto shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
            const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

            for (const auto & key : expired_keys)
                KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);

            block = insertResultsIntoColumns(expired_places, std::move(out_cols), /*arena=*/nullptr);

            /// Remove expired keys from hybrid hash table
            if (auto err = table.removeKeys(expired_keys); err != ErrorCodes::OK)
                throw Exception(
                    err, "Failed to remove keys from hybrid hash table, error_code={}, error={}", err, DB::ErrorCodes::getName(err));

            for (auto & key : expired_keys)
                expired_key_set.insert(std::move(key));
        }

        /// After processing the current insert batch, spill to disk
        table.spillIfNecessary();
    }

    Block block2;
    if (!outstanding_keys.empty())
    {
        /// Handle expired keys
        auto remove_result = outstanding_keys.removeExpiredKeys(params->emit_key_params->timeout_interval_ms, expired_key_set);
        if (remove_result.second != ErrorCodes::OK)
            throw Exception(remove_result.second, "Failed to remove expired keys from HybridKeyList");

        const auto & expired_keys = remove_result.first;

        if (!expired_keys.empty())
        {
            if (!params->emit_key_params->only_max_span)
            {
                auto find_results = table.findKeys(expired_keys);
                if (find_results.hasError())
                    throw Exception(
                        emplace_results.errorCode(),
                        "Failed to find expired keys from HybridHashTable, error={}",
                        emplace_results.errorString());

                PaddedPODArray<ConstAggregateDataPtr> expired_places;
                expired_places.reserve(expired_keys.size());

                OutputBlockColumns out_cols
                    = prepareOutputBlockColumns(getHeader(/*final=*/true), /*aggregates_pools=*/{}, /*final=*/true, expired_keys.size());
                auto shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
                const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

                for (size_t i = 0; const auto & result : find_results.results)
                {
                    if (result.isFound())
                    {
                        KeyGetter::insertKeyIntoColumns(expired_keys[i], out_cols.raw_key_columns, key_sizes_ref);
                        expired_places.emplace_back(reinterpret_cast<ConstAggregateDataPtr>(result.getMapped()));
                    }
                    else
                    {
                        /// XXX, look there are some discrepancy between the in-memory outstanding keys and on disk HashTable
                        LOG_WARNING(logger, "Expired key is missing in HybridHashTable");
                    }

                    ++i;
                }

                block2 = insertResultsIntoColumns(expired_places, std::move(out_cols), /*arena=*/nullptr);
            }

            /// Remove expired keys from hybrid hash table
            if (auto err = table.removeKeys(expired_keys); err != ErrorCodes::OK)
                throw Exception(
                    err, "Failed to remove keys from hybrid hash table, error_code={}, error={}", err, DB::ErrorCodes::getName(err));

            for (auto & key : expired_keys)
                expired_key_set.insert(std::move(key));
        }

        if (!expired_keys.empty())
            table.spillIfNecessary();
    }

    if (!emplace_results.results.empty())
    {
        /// Finally add the new keys in the current insert batch to outstanding key list
        std::vector<typename KeyGetter::KeyType> keys_to_add;
        for (size_t i = 0; auto & emplace_result : emplace_results.results)
        {
            if (emplace_result.isInserted() && !expired_key_set.contains(keys[i]))
                keys_to_add.push_back(std::move(keys[i]));

            ++i;
        }

        if (!keys_to_add.empty())
        {
            if (auto errcode = outstanding_keys.emplace(keys_to_add); errcode != ErrorCodes::OK)
                throw Exception(errcode, "Failed to add keys to HybridKeyList");
        }
    }

    auto block_rows = block.rows();
    auto block2_rows = block2.rows();

    if (block_rows > 0 && block2_rows > 0)
        return concatenateBlocks(std::vector{std::move(block), std::move(block2)});
    else if (block_rows > 0)
        return block;
    else if (block2_rows > 0)
        return block2;
    else
        return block;
}

}

}
