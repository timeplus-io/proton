#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>
#include <Common/PackedTimeBucket.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

namespace Streaming
{

auto MemoryAggregator::getZeroOutWindowKeysFunc(Arena * arena) const
{
    chassert(bucket_key_offset);

    auto window_keys_bytes = params->window_keys_num == 2 ? key_sizes[0] + key_sizes[1] : key_sizes[0];
    bool is_single_key = key_sizes.size() == 1;
    /// In order to merge state with same other keys of different gcd buckets, reset the window group keys to zero
    /// create a new key, where the window key part is 0, and the other key parts are the same as the original value.
    /// For example:
    /// Key :           <window_start> +  <window_end> +  <other_keys...>
    /// New key :                0     +        0      +  <other_keys...>
    return [window_keys_bytes, offset = bucket_key_offset.value(), arena, is_single_key](const auto & key) {
        if constexpr (std::is_same_v<std::decay_t<decltype(key)>, StringRef>)
        {
            /// Case-1: Serialized key, the window time keys always are always lower bits
            auto * data = const_cast<char *>(arena->insert(key.data, key.size));
            chassert(data != nullptr);
            std::memset(data, 0, window_keys_bytes);

            return SerializedKeyHolder{StringRef(data, key.size), *arena};
        }
        else
        {
            /// Case-2: Only one window time key
            if (is_single_key)
                return static_cast<std::decay_t<decltype(key)>>(0);

            /// Case-3: Fixed key
            return zeroOutTimeBucket(key, window_keys_bytes, offset);
        }
    };
}

void MemoryAggregator::mergeWithoutKeyDataImpl(ManyMemoryAggregatedDataVariants & non_empty_data) const
{
    MemoryAggregatedDataVariantsPtr & res = non_empty_data[0];

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        MemoryAggregatedDataVariants & current = *non_empty_data[result_num];
        mergeAggregateStates(res->without_key, current.without_key, res->aggregates_pool);
    }
}

template <bool is_two_level, typename Method, typename Table, typename KeyHandler>
void NO_INLINE MemoryAggregator::mergeUpdatesDataImpl(
    std::vector<Table *> & tables,
    Arena * arena,
    bool use_compiled_functions,
    bool reset_updated,
    AggregatingConvertParams & cparams,
    KeyHandler && key_handler) const
{
    chassert(!tables.empty());
    auto & dst_table = *tables.front();
    /// Always merge updated data into empty first.
    chassert(dst_table.empty());

    /// For example:
    ///                 thread-1        thread-2
    ///     group-1     updated       non-updated
    ///     group-2     non-updated   updated
    ///     group-3     non-updated   non-updated
    /// (Fast path)
    if (cparams.hasKeyUpdated())
    {
        /// 1) Collect all updated groups by keys hint
        /// `dst_table` <= keys hint
        if constexpr (!std::is_same_v<KeyHandler, EmptyKeyHandler>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Merge keys hint with specified key handler is not supported yet");

        for (auto & list_key_cols : cparams.many_updated_keys)
        {
            for (auto & keys : list_key_cols)
            {
                ColumnRawPtrs raw_keys;
                raw_keys.reserve(keys.size());
                for (auto & key : keys)
                    raw_keys.push_back(key.get());

                typename Method::State state(raw_keys, key_sizes, aggregation_state_cache);
                auto rows_num = raw_keys.at(0)->size();
                for (size_t i = 0; i < rows_num; ++i)
                {
                    auto key_holder = state.getKeyHolder(i, *arena);

                    typename Table::LookupResult dst_it;
                    bool inserted;
                    dst_table.emplace(key_holder, dst_it, inserted);
                    if (inserted)
                    {
                        auto & dst = dst_it->getMapped();
                        dst = nullptr; /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                        auto * aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                        createAggregateStates(aggregate_data);
                        dst = aggregate_data;
                    }
                }

                keys.clear(); /// clear the keys to release the memory early
            }
        }
        cparams.reset();
    }
    else
    {
        /// 1) Collect all updated groups by iterating all groups
        /// `dst_table` <= (group-1, group-2)
        for (size_t i = 1; i < tables.size(); ++i)
        {
            auto & src_table = *tables[i];
            auto merge_updated_func = [&](const auto & key, auto & mapped) {
                /// Skip no updated group
                if (!TrackingUpdates::updated(mapped))
                    return;

                typename Table::LookupResult dst_it;
                bool inserted;
                /// For StringRef `key`, it is safe to store to `dst_table`
                /// since the `dst_table` is temporary and the `src_table` will not be cleaned in the meantime
                if constexpr (std::is_same_v<KeyHandler, EmptyKeyHandler>)
                    dst_table.emplace(key, dst_it, inserted);
                else
                    dst_table.emplace(key_handler(key), dst_it, inserted);

                if (inserted)
                {
                    auto & dst = dst_it->getMapped();
                    dst = nullptr; /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                    auto * aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                    createAggregateStates(aggregate_data);
                    dst = aggregate_data;
                }
            };

            if constexpr (is_two_level)
                src_table.forEachValueOfUpdatedBuckets(std::move(merge_updated_func), reset_updated);
            else
                src_table.forEachValue(std::move(merge_updated_func));
        }
    }

    /// 2) Merge all updated groups parts for each thread (based on `1)` )
    /// `dst` <= (thread-1: group-1  group-2) + (thread-2: group-1 group-2)
    for (size_t i = 1; i < tables.size(); ++i)
    {
#if USE_EMBEDDED_COMPILER
        PaddedPODArray<AggregateDataPtr> dst_places;
        PaddedPODArray<AggregateDataPtr> src_places;
#endif

        auto & src_table = *tables[i];

        if constexpr (std::is_same_v<KeyHandler, EmptyKeyHandler>)
        {
            /// a. If use original key, we only loop updated groups `dst_table`, then find and merge states from `src_table`
#if USE_EMBEDDED_COMPILER
            if (use_compiled_functions)
            {
                auto approx_size = dst_table.size();
                dst_places.reserve(approx_size);
                src_places.reserve(approx_size);
            }
#endif

            dst_table.forEachValue([&](const auto & key, auto & mapped) {
                auto find_it = src_table.find(key);
                if (!find_it)
                    return;

#if USE_EMBEDDED_COMPILER
                if (use_compiled_functions)
                {
                    dst_places.push_back(mapped);
                    src_places.push_back(find_it->getMapped());
                }
#endif

                mergeAggregateStates(
                    mapped,
                    find_it->getMapped(),
                    arena,
                    /*skip_compiled_aggregate_functions=*/use_compiled_functions);

                if (reset_updated)
                    TrackingUpdates::resetUpdated(find_it->getMapped());
            });
        }
        else
        {
            /// b. If use handled key (e.g. reset window keys to zero), we loop all groups of `src_table`, then find and merge states of update groups from `dst_table` via handled key
#if USE_EMBEDDED_COMPILER
            if (use_compiled_functions)
            {
                auto approx_size = src_table.size();
                dst_places.reserve(approx_size);
                src_places.reserve(approx_size);
            }
#endif

            src_table.forEachValue([&](const auto & key, auto & mapped) {
                auto key_holder = key_handler(key);
                auto find_it = dst_table.find(keyHolderGetKey(key_holder));
                if (!find_it)
                    return;

#if USE_EMBEDDED_COMPILER
                if (use_compiled_functions)
                {
                    dst_places.push_back(find_it->getMapped());
                    src_places.push_back(mapped);
                }
#endif

                mergeAggregateStates(
                    find_it->getMapped(),
                    mapped,
                    arena,
                    /*skip_compiled_aggregate_functions=*/use_compiled_functions);

                if (reset_updated)
                    TrackingUpdates::resetUpdated(mapped);
            });
        }

#if USE_EMBEDDED_COMPILER
        if (use_compiled_functions)
        {
            const auto & compiled_functions = compiled_aggregate_functions_holder->compiled_aggregate_functions;
            compiled_functions.merge_aggregate_states_function(dst_places.data(), src_places.data(), dst_places.size());
        }
#endif
    }
}

Block MemoryAggregator::spliceAndConvertToBlock(IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const
{
    chassert(data_variants.aggregatorType() == AggregatorType::Memory);
    auto & variants = static_cast<MemoryAggregatedDataVariants &>(data_variants);

    chassert(variants.isTimeBucketTwoLevel());

    bool need_splice = gcd_buckets.size() > 1;
    Arena * arena = variants.aggregates_pool;
    MemoryAggregatedDataVariants res{/*enable_recycle=*/false}; /// This data variants is temporary, disable recycle
    if (need_splice)
    {
        res.aggregator = this;
        initDataVariants(res);
        arena = res.aggregates_pool;
        chassert(res.type == variants.type);
    }

    bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
    if (compiled_aggregate_functions_holder)
        use_compiled_functions = true;
#endif

    AggregatingConvertParams cparams{AggregatingConvertType::Normal};
    switch (variants.type)
    {
#define M(NAME) \
    case MemoryAggregatedDataVariants::Type::NAME: \
    { \
        if (need_splice) \
        { \
            for (auto bucket : gcd_buckets) \
                mergeDataImpl<decltype(variants.NAME)::element_type>( \
                    res.NAME->data.impls[0], \
                    variants.NAME->data.impls[bucket], \
                    arena, \
                    use_compiled_functions, \
                    getZeroOutWindowKeysFunc(arena)); \
\
            return convertOneBucketToBlockImpl(res, *res.NAME, arena, /*final_=*/true, /*bucket=*/0, cparams); \
        } \
        else \
        { \
            return convertOneBucketToBlockImpl(variants, *variants.NAME, arena, /*final_=*/true, gcd_buckets[0], cparams); \
        } \
    }

        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M
        default:
        {
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
        }
    }

    UNREACHABLE();
}

void MemoryAggregator::mergeUpdateGroups(ManyMemoryAggregatedDataVariants & non_empty_data, AggregatingConvertParams & cparams) const
{
    bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
    if (compiled_aggregate_functions_holder)
        use_compiled_functions = true;
#endif

    auto & first = *non_empty_data.at(0);

    switch (first.type)
    {
        case MemoryAggregatedDataVariants::Type::without_key:
        {
            if (std::ranges::none_of(non_empty_data, [](auto & variants) {
                    return variants->without_key && TrackingUpdates::updated(variants->without_key);
                }))
                return;

            mergeWithoutKeyDataImpl(non_empty_data);
            break;
        }

#define M(NAME, IS_TWO_LEVEL) \
    case MemoryAggregatedDataVariants::Type::NAME: \
    { \
        using Method = decltype(first.NAME)::element_type; \
        using Table = std::decay_t<decltype(first.NAME->data)>; \
        std::vector<Table *> tables; \
        tables.reserve(non_empty_data.size()); \
        for (auto & variants : non_empty_data) \
            tables.emplace_back(&(variants->NAME->data)); \
\
        mergeUpdatesDataImpl<IS_TWO_LEVEL, Method>( \
            tables, first.aggregates_pool, use_compiled_functions, /*reset_updated=*/true, cparams); \
        break; \
    }

            APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
        default:
        {
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
        }
    }
}

template <typename Method>
void MemoryAggregator::mergeRetractGroupsImpl(
    ManyMemoryAggregatedDataVariants & non_empty_data, Arena * arena, AggregatingConvertParams & cparams) const
{
    MemoryAggregatedDataVariantsPtr & res = non_empty_data[0];
    auto & dst_table = getDataVariant<Method>(*res).data;
    /// Always merge retract data into empty first.
    chassert(dst_table.empty());

    /// For example:
    ///                 thread-1        thread-2
    ///     group-1     retract       non-retract
    ///     group-2     non-retract   retract
    ///     group-3     non-retract   non-retract
    /// (Fast path)
    if (cparams.hasKeyUpdated())
    {
        /// 1) Collect all retract groups by keys hint
        /// `dst` <= keys hint
        for (auto & list_key_cols : cparams.many_updated_keys)
        {
            for (auto & keys : list_key_cols)
            {
                ColumnRawPtrs raw_keys;
                raw_keys.reserve(keys.size());
                for (auto & key : keys)
                    raw_keys.emplace_back(key.get());

                typename Method::State state(raw_keys, key_sizes, aggregation_state_cache);
                auto rows_num = raw_keys.at(0)->size();
                for (size_t i = 0; i < rows_num; ++i)
                {
                    auto key_holder = state.getKeyHolder(i, *arena);

                    typename Method::Data::LookupResult dst_it;
                    bool inserted;
                    dst_table.emplace(key_holder, dst_it, inserted);
                    if (inserted)
                    {
                        auto & dst = dst_it->getMapped();
                        dst = nullptr; /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                        auto * aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                        createAggregateStates(aggregate_data);
                        dst = aggregate_data;
                    }
                }

                keys.clear(); /// destroy the key columns to release the memory early
            }

            list_key_cols = {};
        }
        cparams.reset();
    }
    else
    {
        /// 1) Collect all retract groups by iterating keys hint
        /// `dst` <= (group-1, group-2)
        using Table = typename Method::Data;
        for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
        {
            auto & src_table = getDataVariant<Method>(*non_empty_data[result_num]).data;
            src_table.forEachValue([&](const auto & key, auto & mapped) {
                /// Skip no retract group
                if (!TrackingUpdatesWithRetract::hasRetract(mapped))
                    return;

                typename Table::LookupResult dst_it;
                bool inserted;
                /// For StringRef `key`, it is safe to store to `dst_table`
                /// since the `dst_table` is temporary and the `src_table` will not be cleaned in the meantime
                dst_table.emplace(key, dst_it, inserted);
                if (inserted)
                {
                    auto & dst = dst_it->getMapped();
                    dst = nullptr; /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                    auto * aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                    createAggregateStates(aggregate_data);
                    dst = aggregate_data;
                }
            });
        }
    }

    /// 2) Merge all retract groups parts for each thread (based on `1)` )
    /// `dst` <= (thread-1: group-1  group-2) + (thread-2: group-1 group-2)
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        auto & current = *non_empty_data[result_num];
        auto & src_table = getDataVariant<Method>(current).data;
        dst_table.forEachValue([&](const auto & key, auto & mapped) {
            if (auto find_it = src_table.find(key))
            {
                auto & src_mapped = find_it->getMapped();
                if (TrackingUpdatesWithRetract::hasRetract(src_mapped))
                {
                    auto & retract = TrackingUpdatesWithRetract::getRetract(src_mapped);
                    mergeAggregateStates(mapped, retract, arena);
                    TrackingUpdatesWithRetract::merge(mapped, retract);
                    destroyAggregateStates(retract); /// Retract data is no longer needed after merged
                }
                else
                {
                    /// If retract data not exist, assume it is not changed, we should use original data
                    mergeAggregateStates(mapped, src_mapped, arena);
                    TrackingUpdatesWithRetract::merge(mapped, src_mapped);
                }
            }
        });

        current.resetAndCreateRetractPool();
    }
}

IAggregatedDataVariantsPtr
MemoryAggregator::mergeGroups(ManyIAggregatedDataVariants & many_data_variants, AggregatingConvertParams & cparams) const
{
    auto prepared_data_ptr = prepareVariantsToMerge(many_data_variants, /*always_merge_into_empty=*/true);
    if (prepared_data_ptr->empty())
        return {};

    if (cparams.type == AggregatingConvertType::Updates || cparams.type == AggregatingConvertType::UpdatesAfterRetract)
    {
        mergeUpdateGroups(*prepared_data_ptr, cparams);
        return prepared_data_ptr->at(0);
    }
    else if (cparams.type == AggregatingConvertType::Retract)
    {
        mergeRetractGroups(*prepared_data_ptr, cparams);
        return prepared_data_ptr->at(0);
    }

    auto & first = *prepared_data_ptr->at(0);
    auto * arena = first.aggregates_pool;
    switch (first.type)
    {
        case MemoryAggregatedDataVariants::Type::without_key:
        {
            mergeWithoutKeyDataImpl(*prepared_data_ptr);
            break;
        }

#define M(NAME) \
    case MemoryAggregatedDataVariants::Type::NAME: \
    { \
        mergeSingleLevelDataImpl<decltype(first.NAME)::element_type>(*prepared_data_ptr); \
        break; \
    }

            APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M

#define M(NAME) \
    case MemoryAggregatedDataVariants::Type::NAME: \
    { \
        using Method = decltype(first.NAME)::element_type; \
        std::vector<Int64> buckets; \
        if (first.isStaticBucketTwoLevel()) \
        { \
            buckets = getDataVariant<Method>(first).data.buckets(); \
        } \
        else \
        { \
            chassert(first.isTimeBucketTwoLevel()); \
            std::unordered_set<Int64> buckets_set; \
            for (const auto & variants : *prepared_data_ptr) \
            { \
                auto tmp_buckets = getDataVariant<Method>(*variants).data.buckets(); \
                buckets_set.insert(tmp_buckets.begin(), tmp_buckets.end()); \
            } \
            buckets.assign(buckets_set.begin(), buckets_set.end()); \
        } \
\
        for (auto bucket : buckets) \
            mergeBucketImpl<Method>(*prepared_data_ptr, bucket, arena); \
\
        break; \
    }

            APPLY_FOR_VARIANTS_ALL_TWO_LEVEL(M)
#undef M
        default:
        {
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
        }
    }

    return prepared_data_ptr->at(0);
}

void MemoryAggregator::mergeRetractGroups(ManyMemoryAggregatedDataVariants & non_empty_data, AggregatingConvertParams & cparams) const
{
    if (unlikely(memory_params->overflow_row))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Overflow row processing is not implemented in streaming aggregation");

    auto & first = *non_empty_data.at(0);
    if (first.type == MemoryAggregatedDataVariants::Type::without_key)
    {
        if (std::ranges::none_of(non_empty_data, [](auto & variants) {
                return variants->without_key && TrackingUpdatesWithRetract::hasRetract(variants->without_key);
            }))
            return; /// Skip if no retracted

        chassert(first.without_key);
        for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
        {
            auto & current = *non_empty_data[result_num];
            chassert(current.without_key);
            if (TrackingUpdatesWithRetract::hasRetract(current.without_key))
            {
                auto & retract = TrackingUpdatesWithRetract::getRetract(current.without_key);
                mergeAggregateStates(first.without_key, retract, first.aggregates_pool);
                TrackingUpdatesWithRetract::merge(first.without_key, retract);
                destroyAggregateStates(retract); /// Retract data is no longer needed after merged
            }
            else
            {
                /// If retract data not exist, assume it is not changed, we should use original data
                mergeAggregateStates(first.without_key, current.without_key, first.aggregates_pool);
                TrackingUpdatesWithRetract::merge(first.without_key, current.without_key);
            }

            current.resetAndCreateRetractPool();
        }
    }

#define M(NAME, IS_TWO_LEVEL) \
    else if (first.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        mergeRetractGroupsImpl<decltype(first.NAME)::element_type>(non_empty_data, first.aggregates_pool, cparams); \
    }

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }
}

Block MemoryAggregator::mergeAndSpliceAndConvertToBlock(
    ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const
{
    bool need_splice = gcd_buckets.size() > 1;
    auto prepared_data_ptr = prepareVariantsToMerge(many_data_variants, /*always_merge_into_empty=*/need_splice);
    if (prepared_data_ptr->empty())
        return {};

    bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
    if (compiled_aggregate_functions_holder)
        use_compiled_functions = true;
#endif

    auto & first = *prepared_data_ptr->at(0);
    chassert(first.isTimeBucketTwoLevel());
    Arena * arena = first.aggregates_pool;
    AggregatingConvertParams cparams{AggregatingConvertType::Normal};

    if (false)
    {
    } // NOLINT
#define M(NAME) \
    else if (first.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        using Method = decltype(first.NAME)::element_type; \
        for (auto bucket : gcd_buckets) \
            mergeBucketImpl<Method>(*prepared_data_ptr, bucket, arena); \
\
        if (need_splice) \
        { \
            for (auto bucket : gcd_buckets) \
                mergeDataImpl<Method>( \
                    first.NAME->data.impls[0], \
                    first.NAME->data.impls[bucket], \
                    arena, \
                    use_compiled_functions, \
                    getZeroOutWindowKeysFunc(arena)); \
\
            return convertOneBucketToBlockImpl(first, *first.NAME, arena, /*final_=*/true, /*bucket=*/0, cparams); \
        } \
        else \
        { \
            return convertOneBucketToBlockImpl(first, *first.NAME, arena, /*final_=*/true, gcd_buckets[0], cparams); \
        } \
    }

    APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }
}

Block MemoryAggregator::spliceAndConvertUpdatesToBlock(
    IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const
{
    bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
    if (compiled_aggregate_functions_holder)
        use_compiled_functions = true;
#endif

    chassert(data_variants.aggregatorType() == AggregatorType::Memory);
    auto & variants = static_cast<MemoryAggregatedDataVariants &>(data_variants);

    chassert(variants.isTimeBucketTwoLevel());
    chassert(needTrackUpdates());

    MemoryAggregatedDataVariants res;
    res.aggregator = this;
    initDataVariants(res);
    chassert(res.type == variants.type);

    Arena * arena = res.aggregates_pool;
    constexpr bool final = true;
    constexpr bool reset_updated = false; /// Don't reset updated, there may be overlapping gcd buckets
    bool need_splice = gcd_buckets.size() > 1;
    AggregatingConvertParams cparams{AggregatingConvertType::Normal};

    switch (variants.type)
    {
#define M(NAME) \
    case MemoryAggregatedDataVariants::Type::NAME: \
    { \
        using Method = decltype(variants.NAME)::element_type; \
        using Table = std::decay_t<decltype(variants.NAME->data.impls[0])>; \
        std::vector<Table *> tables = {nullptr}; \
        tables.reserve(gcd_buckets.size() + 1); \
        for (auto bucket : gcd_buckets) \
            tables.emplace_back(&variants.NAME->data.impls[bucket]); \
        if (need_splice) \
        { \
            tables[0] = &(res.NAME->data.impls[0]); \
            mergeUpdatesDataImpl</*is_two_level=*/false, Method>( \
                tables, arena, use_compiled_functions, reset_updated, cparams, getZeroOutWindowKeysFunc(arena)); \
            return convertOneBucketToBlockImpl(res, *res.NAME, arena, final, /*bucket=*/0, cparams); \
        } \
        else \
        { \
            tables[0] = &(res.NAME->data.impls[gcd_buckets[0]]); \
            mergeUpdatesDataImpl</*is_two_level=*/false, Method>(tables, arena, use_compiled_functions, reset_updated, cparams); \
            return convertOneBucketToBlockImpl(res, *res.NAME, arena, final, gcd_buckets[0], cparams); \
        } \
    }

        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M
        default:
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }

    UNREACHABLE();
}

Block MemoryAggregator::mergeAndSpliceAndConvertUpdatesToBlock(
    ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const
{
    bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
    if (compiled_aggregate_functions_holder)
        use_compiled_functions = true;
#endif

    auto prepared_data_ptr = prepareVariantsToMerge(many_data_variants, /*always_merge_into_empty=*/true);
    if (prepared_data_ptr->empty())
        return {};

    auto & first = *prepared_data_ptr->at(0);
    chassert(first.isTimeBucketTwoLevel());
    chassert(needTrackUpdates());

    Arena * arena = first.aggregates_pool;
    constexpr bool final = true;
    constexpr bool reset_updated = false; /// Don't reset updated, there may be overlapping gcd buckets
    bool need_splice = gcd_buckets.size() > 1;
    ManyMemoryAggregatedDataVariants spliced_variants;
    AggregatingConvertParams cparams{AggregatingConvertType::Normal};

    switch (first.type)
    {
#define M(NAME) \
    case MemoryAggregatedDataVariants::Type::NAME: \
    { \
        using Method = decltype(first.NAME)::element_type; \
        using Table = std::decay_t<decltype(first.NAME->data.impls[0])>; \
        std::vector<Table *> tables{nullptr}; \
        tables.reserve(gcd_buckets.size() * (prepared_data_ptr->size() - 1) + 1); \
        for (auto bucket : gcd_buckets) \
            for (size_t i = 1; i < prepared_data_ptr->size(); ++i) \
                tables.emplace_back(&(prepared_data_ptr->at(i)->NAME->data.impls[bucket])); \
        if (need_splice) \
        { \
            tables[0] = &(first.NAME->data.impls[0]); \
            mergeUpdatesDataImpl</*is_two_level=*/false, Method>( \
                tables, arena, use_compiled_functions, reset_updated, cparams, getZeroOutWindowKeysFunc(arena)); \
            return convertOneBucketToBlockImpl(first, *first.NAME, arena, final, /*bucket=*/0, cparams); \
        } \
        else \
        { \
            tables[0] = &(first.NAME->data.impls[gcd_buckets[0]]); \
            mergeUpdatesDataImpl</*is_two_level=*/false, Method>(tables, arena, use_compiled_functions, reset_updated, cparams); \
            return convertOneBucketToBlockImpl(first, *first.NAME, arena, final, gcd_buckets[0], cparams); \
        } \
    }

        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M
        default:
        {
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
        }
    }
}

}

}
