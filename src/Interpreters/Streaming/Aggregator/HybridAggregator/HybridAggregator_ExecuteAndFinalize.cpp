#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingTime.h>
#include <Common/HybridHashTable/HybridKeyGetter.h>

namespace DB::Streaming
{
Block HybridAggregator::executeAndFinalizeWithoutKeyPerRowImpl(
    AggregateDataPtr aggregate_data, size_t row_begin, size_t row_end, AggregateFunctionInstruction * aggregate_instructions) const
{
    OutputBlockColumns out_cols = prepareOutputBlockColumns(getHeader(), /*aggregates_pool=*/nullptr, row_end - row_begin + 1);
    for (size_t i = row_begin; i < row_end; ++i)
        addAndInsertAggregatesIntoColumns(aggregate_data, aggregate_instructions, out_cols.final_aggregate_columns, i, /*arena=*/nullptr);

    return finalizeBlock(getHeader(), std::move(out_cols), row_end - row_begin);
}

template <typename Table, typename KeyGetter>
Block NO_INLINE HybridAggregator::executeAndFinalizePerRowImpl(
    Table & table,
    const KeyGetter & key_getter,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    std::string_view variants_id) const
{
    Block result_block_header = getHeader();
    OutputBlockColumns out_cols = prepareOutputBlockColumns(result_block_header, /*aggregates_pool=*/nullptr, row_end - row_begin + 1);
    auto shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

    for (size_t i = row_begin; i < row_end; ++i)
    {
        auto key = key_getter.getKeyHolder(i);
        auto emplace_result = table.emplaceKey(key, /*disable_spill=*/false);
        if (emplace_result.hasError())
            throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

        auto * aggregate_data = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());
        chassert(aggregate_data != nullptr);

        /// Insert the key into the output columns.
        KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);

        /// Add and insert aggregates into columns.
        addAndInsertAggregatesIntoColumns(aggregate_data, aggregate_instructions, out_cols.final_aggregate_columns, i, /*arena=*/nullptr);
    }

    table.logMetrics(/*throttling_sec=*/30, "aggr-per-row", variants_id);

    return finalizeBlock(result_block_header, std::move(out_cols), row_end - row_begin);
}

Block HybridAggregator::executeAndFinalizePerRow(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & variants_result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns) const
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
                *result.table.NAME, key_getter, row_begin, row_end, aggregate_functions_instructions.data(), result.getID()); \
        } \
        else \
        { \
            HybridKeyGetter<HybridHashType::NAME, /*nullable=*/false> key_getter{key_columns, key_sizes}; \
            return executeAndFinalizePerRowImpl( \
                *result.table.NAME, key_getter, row_begin, row_end, aggregate_functions_instructions.data(), result.getID()); \
        } \
    }
            APPLY_FOR_HASH_KEY_VARIANTS_HYBRID(M)
#undef M
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} group by key is not supported by `EMIT PER EVENT`", method_chosen);
    }
}

Block HybridAggregator::executeAndFinalizeAfterSessionClose(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    IAggregatedDataVariants & variants_result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns) const
{
    chassert(variants_result.aggregatorType() == AggregatorType::Hybrid);
    auto & result = static_cast<HybridAggregatedDataVariants &>(variants_result);

    result.aggregator = this;
    if (result.empty())
        initStates(result);

    chassert(params->emit_session_params->session_ts_col_pos < columns.size());
    chassert(!params->emit_session_params->session_start_pos || params->emit_session_params->session_start_pos.value() < columns.size());
    chassert(!params->emit_session_params->session_end_pos || params->emit_session_params->session_end_pos.value() < columns.size());

    const auto * session_ts_col = columns[params->emit_session_params->session_ts_col_pos].get();
    const IColumn * session_start_col
        = params->emit_session_params->session_start_pos ? columns[params->emit_session_params->session_start_pos.value()].get() : nullptr;
    const IColumn * session_end_col
        = params->emit_session_params->session_end_pos ? columns[params->emit_session_params->session_end_pos.value()].get() : nullptr;

    /// Constant columns are not supported directly during aggregation.
    /// To make them work anyway, we materialize them.
    Columns materialized_columns = materializeKeyColumns(columns, key_columns, result.isLowCardinality());

    NestedColumnsHolder nested_columns_holder;
    AggregateFunctionInstructions aggregate_functions_instructions;
    prepareAggregateInstructions(columns, aggregate_columns, materialized_columns, aggregate_functions_instructions, nested_columns_holder);

    switch (method_chosen)
    {
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} group by key is not supported by `EMIT AFTER SESSION CLOSE`", method_chosen);

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        if (has_nullable_key) \
        { \
            HybridKeyGetter<HybridHashType::NAME, /*nullable=*/true> key_getter{key_columns, key_sizes}; \
            return executeAndFinalizeAfterSessionCloseImpl( \
                *result.table.NAME, \
                *result.outstanding_keys.NAME, \
                session_ts_col, \
                session_start_col, \
                session_end_col, \
                key_getter, \
                row_begin, \
                row_end, \
                aggregate_functions_instructions.data(), \
                result.getID()); \
        } \
        else \
        { \
            HybridKeyGetter<HybridHashType::NAME, /*nullable=*/false> key_getter{key_columns, key_sizes}; \
            return executeAndFinalizeAfterSessionCloseImpl( \
                *result.table.NAME, \
                *result.outstanding_keys.NAME, \
                session_ts_col, \
                session_start_col, \
                session_end_col, \
                key_getter, \
                row_begin, \
                row_end, \
                aggregate_functions_instructions.data(), \
                result.getID()); \
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
[[nodiscard]] Block NO_INLINE HybridAggregator::executeAndFinalizeAfterSessionCloseImpl(
    Table & table,
    KeyList & outstanding_keys,
    const IColumn * session_ts_col,
    const IColumn * session_start_col,
    const IColumn * session_end_col,
    const KeyGetter & key_getter,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    std::string_view variants_id) const
{
    if (session_start_col || session_end_col)
        /// Have session start / end condition columns, it is usually more complex and slower
        return executeAndFinalizeAfterSessionCondCloseImpl(
            table,
            outstanding_keys,
            session_ts_col,
            session_start_col,
            session_end_col,
            key_getter,
            row_begin,
            row_end,
            aggregate_instructions,
            variants_id);

    /// Faster path when session start / end columns are absent
    ///
    /// NOTE: only row_end-row_start is required, but:
    /// - this affects only optimize_aggregation_in_order,
    /// - this is just a pointer, so it should not be significant,
    /// - and plus this will require other changes in the interface.
    std::unique_ptr<AggregateDataPtr[]> places;
    std::vector<typename KeyGetter::KeyType> keys;

    absl::flat_hash_set<typename KeyGetter::KeyType> handled_key_set;
    HybridEmplaceResults emplace_results;

    Block block;
    if (auto rows = row_end - row_begin; rows > 0)
    {
        keys.reserve(rows);

        for (size_t row = row_begin; row < row_end; ++row)
            keys.emplace_back(key_getter.getKeyHolder(row));

        emplace_results = table.emplaceKeys(keys);
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

        /// 1) Check sessions which reach max duration in the current insert batch
        std::vector<typename KeyGetter::KeyType> max_duration_keys;
        PaddedPODArray<ConstAggregateDataPtr> max_duration_places;
        auto * places_ptr = places.get();

        for (size_t i = 0, row = row_begin; row < row_end; ++row, ++i)
        {
            TrackingTime::updateTimestamp(places_ptr[row], session_ts_col->get64(row));

            if (TrackingTime::maxSpanReached(places_ptr[row], params->emit_session_params->max_span_interval))
            {
                if (!emplace_results.results[i].isInserted())
                {
                    /// Remove existing key from the outstanding key list
                    auto create_utc_ts = TrackingTime::getCreateTimestamp(places_ptr[row]);
                    if (auto errcode = outstanding_keys.removeKey(keys[i], create_utc_ts); errcode != ErrorCodes::OK)
                        throw Exception(
                            errcode,
                            "Failed to remove key from hybrid key list, error_code={}, error={}",
                            errcode,
                            DB::ErrorCodes::getName(errcode));
                }

                max_duration_keys.push_back(std::move(keys[i]));
                max_duration_places.emplace_back(places_ptr[row]);
            }
        }

        if (!max_duration_keys.empty())
        {
            OutputBlockColumns out_cols = prepareOutputBlockColumns(getHeader(), /*aggregates_pool=*/nullptr, row_end - row_begin + 1);
            auto shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
            const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

            for (const auto & key : max_duration_keys)
                KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);

            block = insertResultsIntoColumns(max_duration_places, std::move(out_cols), /*arena=*/nullptr);

            /// Remove sessions which reaches max durations from hybrid hash table
            if (auto err = table.removeKeys(max_duration_keys); err != ErrorCodes::OK)
                throw Exception(
                    err, "Failed to remove keys from hybrid hash table, error_code={}, error={}", err, DB::ErrorCodes::getName(err));

            for (auto & key : max_duration_keys)
                handled_key_set.insert(std::move(key));
        }
    }

    bool removed_expired_sessions = false;
    Block block2 = finalizeExpiredSessions(
        table,
        key_getter,
        outstanding_keys,
        handled_key_set,
        aggregate_instructions,
        /*add_expired_keys=*/true,
        removed_expired_sessions);

    if (row_end > row_begin || removed_expired_sessions)
        table.spillIfNecessary();

    if (!emplace_results.results.empty())
    {
        /// Finally add the new keys in the current insert batch to outstanding key list
        std::vector<typename KeyGetter::KeyType> keys_to_add;
        keys_to_add.reserve(emplace_results.results.size() / 2 + 1);

        std::vector<size_t> inserted;
        inserted.reserve(emplace_results.results.size() / 2 + 1);

        for (size_t i = 0; auto & emplace_result : emplace_results.results)
        {
            if (emplace_result.isInserted() && !handled_key_set.contains(keys[i]))
            {
                keys_to_add.push_back(std::move(keys[i]));
                inserted.push_back(i);
            }

            ++i;
        }

        if (!keys_to_add.empty())
        {
            if (auto [create_utc_ts, errcode] = outstanding_keys.emplaceKeys(keys_to_add); errcode == ErrorCodes::OK)
            {
                /// Update the create timestamp which will be used to delete keys from HybridKeyList earlier
                /// if session reaches maxspan or closed before it expires
                for (auto i : inserted)
                    TrackingTime::setCreateTimestamp(
                        static_cast<AggregateDataPtr>(emplace_results.results[i].getMutableMapped()), create_utc_ts);
            }
            else
            {
                throw Exception(errcode, "Failed to add keys to HybridKeyList, error={}", DB::ErrorCodes::getName(errcode));
            }
        }
    }

    table.logMetrics(/*throttling_sec=*/30, "aggr-after-session-close", variants_id);
    /// outstanding_keys.logMetrics(/*throttling_sec=*/30, "timed-session-keys", variants_id);

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

template <typename Table, typename KeyList, typename KeyGetter>
Block HybridAggregator::finalizeExpiredSessions(
    Table & table,
    const KeyGetter &,
    KeyList & outstanding_keys,
    absl::flat_hash_set<typename KeyGetter::KeyType> & handled_key_set,
    [[maybe_unused]] AggregateFunctionInstruction * aggregate_instructions,
    bool add_expired_keys,
    bool & removed_expired_session) const
{
    if (outstanding_keys.empty())
        return {};

    /// Handle expired keys
    auto remove_result = outstanding_keys.removeTimedOutKeys(params->emit_session_params->timeout_interval_ms, handled_key_set);
    if (remove_result.second != ErrorCodes::OK)
        throw Exception(remove_result.second, "Failed to remove expired keys from HybridKeyList");

    const auto & expired_keys = remove_result.first;
    if (expired_keys.empty())
        return {};

    Block block;
    if (!params->emit_session_params->only_max_span)
    {
        auto find_results = table.findKeys(expired_keys, /*disable_spill=*/true);
        if (find_results.hasError())
            throw Exception(
                find_results.errorCode(), "Failed to find expired keys from HybridHashTable, error={}", find_results.errorString());

        PaddedPODArray<ConstAggregateDataPtr> expired_places;
        expired_places.reserve(expired_keys.size());

        OutputBlockColumns out_cols = prepareOutputBlockColumns(getHeader(), /*aggregates_pool=*/nullptr, expired_keys.size());
        auto shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
        const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

        for (size_t i = 0; const auto & result : find_results.results)
        {
            if (result.isFound())
            {
                /// Recheck the expiration / timeout ?
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

        block = insertResultsIntoColumns(expired_places, std::move(out_cols), /*arena=*/nullptr);
    }

    removed_expired_session = true;

    /// Remove expired keys from hybrid hash table
    if (auto err = table.removeKeys(expired_keys); err != ErrorCodes::OK)
        throw Exception(
            err,
            "Failed to remove {} keys from hybrid hash table, error_code={}, error={}",
            expired_keys.size(),
            err,
            DB::ErrorCodes::getName(err));

    if (add_expired_keys)
    {
        for (auto & key : expired_keys)
            handled_key_set.insert(std::move(key));
    }

    return block;
}

template <typename Table, typename KeyList, typename KeyGetter>
[[nodiscard]] Block NO_INLINE HybridAggregator::executeAndFinalizeAfterSessionCondCloseImpl(
    Table & table,
    KeyList & outstanding_keys,
    const IColumn * session_ts_col,
    const IColumn * session_start_col,
    const IColumn * session_end_col,
    const KeyGetter & key_getter,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    [[maybe_unused]] std::string_view variants_id) const
{
    Block result_block_header = getHeader();
    OutputBlockColumns out_cols = prepareOutputBlockColumns(result_block_header, /*aggregates_pool=*/nullptr, row_end - row_begin + 1);
    auto shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

    absl::flat_hash_set<typename KeyGetter::KeyType> handled_key_set;

    auto emplace_new_and_aggregate = [&](size_t row_num, const KeyGetter::KeyType & k, bool add_to_outstanding_keys = true) {
        auto emplace_result = table.emplaceNewKey(k, /*disable_spill=*/true);
        if (emplace_result.hasError())
            throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

        auto * aggregate_data = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());
        chassert(aggregate_data != nullptr);

        aggregateSingleRow(aggregate_data, aggregate_instructions, row_num, /*arena=*/nullptr);
        TrackingTime::updateTimestamp(aggregate_data, session_ts_col->get64(row_num));

        if (add_to_outstanding_keys)
        {
            if (auto [create_utc_ts, errcode] = outstanding_keys.emplaceKey(k); errcode == ErrorCodes::OK)
                TrackingTime::setCreateTimestamp(aggregate_data, create_utc_ts);
            else
                throw Exception(errcode, "Failed to insert key to hybrid key list");
        }

        return aggregate_data;
    };

    /// For an existing session, if max span reaches, finalize and close the session
    auto aggregate_and_finalize = [&](size_t row_num, const KeyGetter::KeyType & k, HybridFindResult & find_result) {
        auto * aggregate_data = static_cast<AggregateDataPtr>(find_result.getMutableMapped());

        /// Add the current row to the existing session window
        aggregateSingleRow(aggregate_data, aggregate_instructions, row_num, /*arena=*/nullptr);
        TrackingTime::updateTimestamp(aggregate_data, session_ts_col->get64(row_num));

        if (TrackingTime::maxSpanReached(aggregate_data, params->emit_session_params->max_span_interval))
        {
            /// If it already reaches max span, close it avoid infinity session

            /// Insert the key into the output columns.
            KeyGetter::insertKeyIntoColumns(k, out_cols.raw_key_columns, key_sizes_ref);

            /// Insert the aggregates to columns
            insertAggregatesIntoColumns(aggregate_data, out_cols.final_aggregate_columns, /*arena=*/nullptr);

            auto create_utc_ts = TrackingTime::getCreateTimestamp(aggregate_data);
            if (auto errcode = outstanding_keys.removeKey(k, create_utc_ts); errcode != ErrorCodes::OK)
                throw Exception(errcode, "Failed to remove session key from hybrid key list, key_create_ts={}", create_utc_ts);

            /// Close the session by removing it
            if (auto errcode = table.removeKey(k); errcode != ErrorCodes::OK)
                throw Exception(errcode, "Failed to remove closed session");

            handled_key_set.insert(k);
        }
    };

    /// For a new session
    auto start_new_session = [&](size_t row_num, const KeyGetter::KeyType & k, HybridFindResult & find_result) {
        if (find_result.isNotFound())
        {
            /// Start a new session
            emplace_new_and_aggregate(row_num, k);
        }
        else
        {
            /// There is already a outstanding session and need start a new one
            auto * aggregate_data = static_cast<AggregateDataPtr>(find_result.getMutableMapped());
            chassert(aggregate_data != nullptr);

            if (!params->emit_session_params->merge_open_sessions)
            {
                if (!params->emit_session_params->only_max_span)
                {
                    /// Insert the key into the output columns.
                    KeyGetter::insertKeyIntoColumns(k, out_cols.raw_key_columns, key_sizes_ref);

                    /// Insert the aggregates to columns
                    insertAggregatesIntoColumns(aggregate_data, out_cols.final_aggregate_columns, /*arena=*/nullptr);
                }

                auto create_utc_ts = TrackingTime::getCreateTimestamp(aggregate_data);
                if (auto errcode = outstanding_keys.removeKey(k, create_utc_ts); errcode != ErrorCodes::OK)
                    throw Exception(errcode, "Failed to remove session key from hybrid key list, key_create_ts={}", create_utc_ts);

                /// Close the session by removing it
                if (auto errcode = table.removeKey(k); errcode != ErrorCodes::OK)
                    throw Exception(errcode, "Failed to remove closed session from hybrid hash table");

                handled_key_set.insert(k);

                /// Start a new one
                emplace_new_and_aggregate(row_num, k);
            }
            else
            {
                /// Merge it to the existing session window
                aggregate_and_finalize(row_num, k, find_result);
            }
        }
    };

    /// For an ended session
    auto finalize_ended_session = [&](size_t row_num, const KeyGetter::KeyType & k, HybridFindResult & find_result) {
        if (find_result.isFound())
        {
            auto * aggregate_data = static_cast<AggregateDataPtr>(find_result.getMutableMapped());

            if (params->emit_session_params->include_session_end)
            {
                /// Add the current row to the existing session window first
                aggregateSingleRow(aggregate_data, aggregate_instructions, row_num, /*arena=*/nullptr);
                TrackingTime::updateTimestamp(aggregate_data, session_ts_col->get64(row_num));
            }

            /// Finalize the session
            if (!params->emit_session_params->only_max_span
                || TrackingTime::maxSpanReached(aggregate_data, params->emit_session_params->max_span_interval))
            {
                /// Insert the key into the output columns.
                KeyGetter::insertKeyIntoColumns(k, out_cols.raw_key_columns, key_sizes_ref);

                /// Insert the aggregates to columns
                insertAggregatesIntoColumns(aggregate_data, out_cols.final_aggregate_columns, /*arena=*/nullptr);
            }

            auto create_utc_ts = TrackingTime::getCreateTimestamp(aggregate_data);
            if (auto errcode = outstanding_keys.removeKey(k, create_utc_ts); errcode != ErrorCodes::OK)
                throw Exception(errcode, "Failed to remove session key from hybrid key list, key_create_ts={}", create_utc_ts);

            /// Close the session by removing it
            if (auto errcode = table.removeKey(k); errcode != ErrorCodes::OK)
                throw Exception(errcode, "Failed to remove closed session from hybrid hash table");

            handled_key_set.insert(k);
        }
        else
        {
            if (!params->emit_session_params->include_session_end)
                return;

            /// There is no session for this key. The session end event is the only row. Finalize it
            auto * aggregate_data = emplace_new_and_aggregate(row_num, k, /*add_to_outstanding_keys=*/false);

            /// Finalize the session
            {
                /// Insert the key into the output columns.
                KeyGetter::insertKeyIntoColumns(k, out_cols.raw_key_columns, key_sizes_ref);

                /// Insert the aggregates to columns
                insertAggregatesIntoColumns(aggregate_data, out_cols.final_aggregate_columns, /*arena=*/nullptr);
            }

            /// Close the session by removing it
            if (auto errcode = table.removeKey(k); errcode != ErrorCodes::OK)
                throw Exception(errcode, "Failed to remove closed session");
        }
    };

    for (size_t i = 0, row = row_begin; row < row_end; ++row, ++i)
    {
        auto key = key_getter.getKeyHolder(row);
        auto find_result = table.findKey(key, /*disable_spill=*/true);
        if (find_result.hasError())
            throw Exception::createRuntime(find_result.errcode, find_result.errorString());

        if (session_start_col && session_end_col)
        {
            auto session_started = session_start_col->getBool(i);
            auto session_ended = session_end_col->getBool(i);

            if (session_ended)
            {
                /// If both session_started and session_ended are true for the same event, honor session_ended is good enough
                finalize_ended_session(row, key, find_result);
            }
            else if (session_started)
            {
                start_new_session(row, key, find_result);
            }
            else
            {
                if (find_result.isFound())
                {
                    /// Session already started, add it to the existing session
                    aggregate_and_finalize(row, key, find_result);
                }
                else
                {
                    /// Session is not started and it is not a session start event, drop it on the floor
                }
            }
        }
        else if (session_start_col)
        {
            auto session_started = session_start_col->getBool(i);
            if (session_started)
            {
                start_new_session(row, key, find_result);
            }
            else
            {
                if (find_result.isFound())
                {
                    /// Session already started, add it to the existing session
                    aggregate_and_finalize(row, key, find_result);
                }
                else
                {
                    /// There is no session for the current key, and it is not a session start event, drop the current row on the floor
                }
            }
        }
        else
        {
            chassert(session_end_col);

            auto session_ended = session_end_col->getBool(i);
            if (session_ended)
            {
                finalize_ended_session(row, key, find_result);
            }
            else
            {
                if (find_result.isFound())
                {
                    /// Session already started, add it to the existing session
                    aggregate_and_finalize(row, key, find_result);
                }
                else
                {
                    /// Session not exists, since there is no session start cond, all events before session end are included in the session
                    start_new_session(row, key, find_result);
                }
            }
        }
    }

    auto rows = out_cols.raw_key_columns.front()->size();

    Block block;
    if (rows > 0)
        block = finalizeBlock(result_block_header, std::move(out_cols), rows);

    bool removed_expired_sessions = false;
    Block block2 = finalizeExpiredSessions(
        table, key_getter, outstanding_keys, handled_key_set, aggregate_instructions, /*add_expired_keys=*/false, removed_expired_sessions);

    if (row_end > row_begin || removed_expired_sessions)
        table.spillIfNecessary();

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
