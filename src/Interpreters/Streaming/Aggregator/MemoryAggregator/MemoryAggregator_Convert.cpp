#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>
#include <Common/CurrentMetrics.h>
#include <Common/scope_guard_safe.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace ProfileEvents
{
extern const Event ExternalAggregationWritePart;
extern const Event ExternalAggregationCompressedBytes;
extern const Event ExternalAggregationUncompressedBytes;
extern const Event ExternalProcessingCompressedBytesTotal;
extern const Event ExternalProcessingUncompressedBytesTotal;
}

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

namespace Streaming
{

namespace
{
inline bool worthConvertToTwoLevel(
    size_t group_by_two_level_threshold, size_t result_size, size_t group_by_two_level_threshold_bytes, Int64 result_size_bytes)
{
    /// params->group_by_two_level_threshold will be equal to 0 if we have only one thread to execute aggregation (refer to AggregatingStep::transformPipeline).
    return (group_by_two_level_threshold && result_size >= group_by_two_level_threshold)
        || (group_by_two_level_threshold_bytes && result_size_bytes >= static_cast<Int64>(group_by_two_level_threshold_bytes));
}

template <typename BucketConverter>
BlocksList convertBucketsInParallel(ThreadPool * thread_pool, const std::vector<Int64> & buckets, BucketConverter && bucket_converter)
{
    std::atomic<UInt32> next_bucket_idx_to_merge = 0;
    auto converter = [&](const std::atomic_flag * cancelled) {
        BlocksList blocks;
        while (true)
        {
            if (cancelled && cancelled->test())
                break;

            UInt32 bucket_idx = next_bucket_idx_to_merge.fetch_add(1);
            if (bucket_idx >= buckets.size())
                break;

            auto bucket = buckets[bucket_idx];
            blocks.splice(blocks.end(), bucket_converter(bucket));
        }
        return blocks;
    };

    size_t num_threads = thread_pool ? std::min(thread_pool->getMaxThreads(), buckets.size()) : 1;
    if (num_threads <= 1)
    {
        return converter(nullptr);
    }

    /// Process in parallel
    auto results = std::make_shared<std::vector<BlocksList>>();
    results->resize(num_threads);
    thread_pool->setMaxThreads(num_threads);
    {
        std::atomic_flag cancelled;
        SCOPE_EXIT_SAFE({ cancelled.test_and_set(); });

        for (size_t thread_id = 0; thread_id < num_threads; ++thread_id)
        {
            thread_pool->scheduleOrThrowOnError([thread_id, thread_group = CurrentThread::getGroup(), results, &converter, &cancelled] {
                SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););
                if (thread_group)
                    CurrentThread::attachToGroupIfDetached(thread_group);

                (*results)[thread_id] = converter(&cancelled);
            });
        }

        thread_pool->wait();
    }

    BlocksList blocks;
    for (auto & result : *results)
        blocks.splice(blocks.end(), std::move(result));

    return blocks;
}

}

BlocksList
MemoryAggregator::convertToBlocks(IAggregatedDataVariants & data_variants, size_t max_threads, AggregatingConvertParams & cparams) const
{
    if (cparams.type == AggregatingConvertType::Retract)
    {
        BlocksList blocks;

        auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);

        /// First collect convert retract aggregate states to blocks
        {
            /// Copy the params to copy the many_updated_keys since it is mutated in-place
            auto retract_cparams = cparams;
            auto retract_blocks = doConvertToBlocks(data_variants, max_threads, retract_cparams);
            for (auto & retract_block : retract_blocks)
            {
                if (auto rows = retract_block.rows(); rows > 0)
                {
                    retract_block.insert(
                        ColumnWithTypeAndName{ColumnInt8::create(rows, static_cast<Int8>(-1)), delta_col_type, "_tp_delta"});
                    retract_block.setRetract();
                    blocks.push_back(std::move(retract_block));
                }
            }
        }

        /// Then collect updated aggregate states to blocks
        {
            AggregatingConvertParams update_cparams{
                AggregatingConvertType::UpdatesAfterRetract, std::move(cparams.many_updated_keys), cparams.keys_already_sharded};

            auto updated_blocks = doConvertToBlocks(data_variants, max_threads, update_cparams);
            for (auto & updated_block : updated_blocks)
            {
                if (auto rows = updated_block.rows(); rows > 0)
                {
                    updated_block.insert(
                        ColumnWithTypeAndName{ColumnInt8::create(rows, static_cast<Int8>(1)), delta_col_type, "_tp_delta"});
                    blocks.push_back(std::move(updated_block));
                }
            }
        }
        return blocks;
    }

    /// There are 2 cases we will need clear aggregate states in variants (hash table)
    /// 1) Global aggregation over global aggregation. With `emit on update`, global aggregation over
    ///    global aggregation is broken now since query like `SELECT sum(s) FROM (SELECT sum(i) AS s FROM stream GROUP BY id EMIT ON UPDATE)`
    ///    can't get correct result and we shall error out with UNSUPPORTED exception
    /// 2) Delta emit. SELECT sum(i) as s FROM stream GROUP BY id EMIT DELTA PERIODIC 1s;
    /// FIXME, refactor cparams.clear_state
    SCOPE_EXIT({
        bool clear_states = cparams.type == AggregatingConvertType::Normal && cparams.clear_state;
        if (clear_states)
            data_variants.reset();
    });

    return doConvertToBlocks(data_variants, max_threads, cparams);
}

BlocksList
MemoryAggregator::doConvertToBlocks(IAggregatedDataVariants & variants, size_t max_threads, AggregatingConvertParams & cparams) const
{
    chassert(variants.aggregatorType() == AggregatorType::Memory);
    auto & data_variants = static_cast<MemoryAggregatedDataVariants &>(variants);

    LOG_DEBUG(logger, "Converting aggregated {} data to blocks", magic_enum::enum_name(cparams.type));

    Stopwatch watch;

    BlocksList blocks;

    /// In what data structure is the data aggregated?
    if (data_variants.empty())
        return blocks;

    if (cparams.type == AggregatingConvertType::Updates || cparams.type == AggregatingConvertType::UpdatesAfterRetract
        || cparams.type == AggregatingConvertType::Retract)
        max_threads = 1;

    if (data_variants.type == MemoryAggregatedDataVariants::Type::without_key)
        blocks.emplace_back(prepareBlockAndFillWithoutKey(data_variants, cparams));
    else if (!data_variants.isTwoLevel())
        blocks = prepareBlockAndFillSingleLevel(data_variants, cparams);
    else
        blocks = prepareBlocksAndFillTwoLevel(data_variants, max_threads, cparams);

    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & block : blocks)
    {
        rows += block.rows();
        bytes += block.bytes();
    }

    double elapsed_seconds = watch.elapsedSeconds();
    LOG_DEBUG(
        logger,
        "Converted aggregated data to blocks. {} rows, {} in {} sec. ({:.3f} rows/sec., {}/sec.)",
        rows,
        ReadableSize(bytes),
        elapsed_seconds,
        rows / elapsed_seconds,
        ReadableSize(bytes / elapsed_seconds));

    return blocks;
}

BlocksList MemoryAggregator::mergeAndConvertToBlocks(
    ManyIAggregatedDataVariants & many_data_variants, size_t max_threads, AggregatingConvertParams & cparams) const
{
    if (cparams.type == AggregatingConvertType::Updates)
    {
        auto merged_updated_data = mergeGroups(many_data_variants, cparams);
        if (merged_updated_data)
        {
            AggregatingConvertParams merged_cparams{AggregatingConvertType::Normal};
            return doConvertToBlocks(*merged_updated_data, max_threads, merged_cparams);
        }
        return {};
    }
    else if (cparams.type == AggregatingConvertType::Retract)
    {
        BlocksList blocks;

        auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);

        /// First collect convert retract aggregate states to blocks
        {
            /// Copy the params to copy the many_updated_keys since it is mutated in-place
            auto retract_cparams = cparams;
            auto merged_retracted_data = mergeGroups(many_data_variants, retract_cparams);
            if (merged_retracted_data)
            {
                chassert(merged_retracted_data->aggregatorType() == AggregatorType::Memory);
                auto & merged_memory = static_cast<MemoryAggregatedDataVariants &>(*merged_retracted_data);

                /// without_key special case: mergeRetractGroups bails when no shard has a retract yet
                /// (e.g. first changelog emit) and leaves first.without_key at the init-empty state.
                /// Converting that as Normal would emit a phantom (…, 0, -1) row — skip it.
                const bool empty_without_key
                    = merged_memory.type == MemoryAggregatedDataVariants::Type::without_key && merged_memory.without_key
                    && TrackingUpdatesWithRetract::empty(merged_memory.without_key);

                if (!empty_without_key)
                {
                    /// Use UpdatesAfterRetract so the keyed conversion filters out merged entries whose
                    /// prior-state contribution sums to empty — a fresh key inserted in this cycle on
                    /// one shard with no carry-over from any other shard.
                    AggregatingConvertParams merged_cparams{AggregatingConvertType::UpdatesAfterRetract};
                    auto retract_blocks = doConvertToBlocks(merged_memory, max_threads, merged_cparams);
                    for (auto & retract_block : retract_blocks)
                    {
                        auto rows = retract_block.rows();
                        retract_block.insert(
                            ColumnWithTypeAndName{ColumnInt8::create(rows, static_cast<Int8>(-1)), delta_col_type, "_tp_delta"});
                        retract_block.setRetract();
                    }

                    blocks.splice(blocks.end(), std::move(retract_blocks));
                }
            }
        }

        /// Then collect updated aggregate states to blocks
        {
            AggregatingConvertParams update_cparams{
                AggregatingConvertType::UpdatesAfterRetract, std::move(cparams.many_updated_keys), cparams.keys_already_sharded};
            auto merged_updated_data = mergeGroups(many_data_variants, update_cparams);
            if (merged_updated_data)
            {
                AggregatingConvertParams merged_cparams{AggregatingConvertType::Normal};
                auto updated_blocks = doConvertToBlocks(*merged_updated_data, max_threads, merged_cparams);
                for (auto & updated_block : updated_blocks)
                {
                    auto rows = updated_block.rows();
                    updated_block.insert(
                        ColumnWithTypeAndName{ColumnInt8::create(rows, static_cast<Int8>(1)), delta_col_type, "_tp_delta"});
                }

                blocks.splice(blocks.end(), std::move(updated_blocks));
            }
        }

        return blocks;
    }

    chassert(cparams.type == AggregatingConvertType::Normal);

    auto prepared_data_ptr = prepareVariantsToMerge(many_data_variants);
    if (prepared_data_ptr->empty())
        return {};

    SCOPE_EXIT({
        /// So far, only EMIT DELTA requires clear state
        if (cparams.clear_state)
        {
            for (auto & variants : *prepared_data_ptr)
                variants->reset();
        }
    });

    if (prepared_data_ptr->size() == 1)
        return doConvertToBlocks(*prepared_data_ptr->at(0), max_threads, cparams);

    BlocksList blocks;
    auto & first = *prepared_data_ptr->at(0);
    if (first.type == MemoryAggregatedDataVariants::Type::without_key)
    {
        mergeWithoutKeyDataImpl(*prepared_data_ptr);
        blocks.emplace_back(prepareBlockAndFillWithoutKey(first, cparams));
    }
    else if (!first.isTwoLevel())
    {
        if (false)
        {
        } // NOLINT
#define M(NAME) \
    else if (first.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        mergeSingleLevelDataImpl<decltype(first.NAME)::element_type>(*prepared_data_ptr); \
    }

        APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
        else
        {
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown single level aggregated data variant.");
        }

        blocks = prepareBlockAndFillSingleLevel(first, cparams);
    }
    else
    {
        return mergeAndConvertTwoLevelToBlocks(prepared_data_ptr, max_threads);
    }

    return blocks;
}

template <bool return_single_block, typename Method, typename Table>
MemoryAggregator::ConvertToBlockRes<return_single_block> MemoryAggregator::convertToBlockImpl(
    Method & method, Table & data, const ArenaPtr & pool, size_t rows, AggregatingConvertParams & cparams) const
{
    if (data.empty())
    {
        auto && out_cols = prepareOutputBlockColumns(getHeader(), pool, rows);
        return {finalizeBlock(getHeader(), std::move(out_cols), rows)};
    }

    ConvertToBlockRes<return_single_block> res;
    bool use_compiled_functions = false;

#if USE_EMBEDDED_COMPILER
    use_compiled_functions = compiled_aggregate_functions_holder != nullptr && !Method::low_cardinality_optimization;
#endif
    res = convertToBlockImplFinal<return_single_block>(method, data, pool, rows, cparams, use_compiled_functions);
    return res;
}

template <typename Method, typename Table, typename ConvertCallback>
void MemoryAggregator::convertState(Table & data, AggregatingConvertParams & cparams, ConvertCallback && callback) const
{
    Arena arena;

    /// (Fast path) If there are cached keys, we can use them to iterate over the data instead of looping over the entire hash table.
    if (cparams.hasKeyUpdated())
    {
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
                if (cparams.type == AggregatingConvertType::Updates)
                {
                    for (size_t i = 0; i < rows_num; ++i)
                    {
                        auto key_holder = state.getKeyHolder(i, arena);
                        auto lookup_result = data.find(keyHolderGetKey(key_holder));
                        chassert(lookup_result);
                        auto & mapped = lookup_result->getMapped();
                        if (!TrackingUpdates::updated(mapped))
                            continue;

                        callback(keyHolderGetKey(key_holder), mapped);
                        TrackingUpdates::resetUpdated(mapped);
                    }
                }
                else if (cparams.type == AggregatingConvertType::UpdatesAfterRetract)
                {
                    for (size_t i = 0; i < rows_num; ++i)
                    {
                        auto key_holder = state.getKeyHolder(i, arena);
                        auto lookup_result = data.find(keyHolderGetKey(key_holder));
                        chassert(lookup_result);
                        auto & mapped = lookup_result->getMapped();
                        chassert(mapped);
                        if (!TrackingUpdatesWithRetract::updated(mapped))
                            continue;

                        /// FIXME: If the key group is empty, also remove the key
                        if (!TrackingUpdatesWithRetract::empty(mapped))
                            callback(keyHolderGetKey(key_holder), mapped);

                        TrackingUpdatesWithRetract::resetUpdated(mapped);
                    }
                }
                else if (cparams.type == AggregatingConvertType::Retract)
                {
                    for (size_t i = 0; i < rows_num; ++i)
                    {
                        auto key_holder = state.getKeyHolder(i, arena);
                        auto lookup_result = data.find(keyHolderGetKey(key_holder));
                        chassert(lookup_result);
                        auto & mapped = lookup_result->getMapped();
                        chassert(mapped);
                        if (!TrackingUpdatesWithRetract::hasRetract(mapped))
                            continue;

                        callback(keyHolderGetKey(key_holder), TrackingUpdatesWithRetract::getRetract(mapped));
                    }
                }
                else
                {
                    chassert(false && "Keys hint is only supported for converting updates and retract");
                }

                keys = {}; /// destroy the key columns to release the memory early
            }

            list_key_cols = {};
        }

        cparams.reset();
        return;
    }

    switch (cparams.type)
    {
        case AggregatingConvertType::Normal:
        {
            data.forEachValue([&](const auto & key, auto & mapped) { callback(key, mapped); });
            break;
        }
        case AggregatingConvertType::Updates:
        {
            data.forEachValue([&](const auto & key, auto & mapped) {
                if (!TrackingUpdates::updated(mapped))
                    return;

                callback(key, mapped);
                TrackingUpdates::resetUpdated(mapped);
            });
            break;
        }
        case AggregatingConvertType::UpdatesAfterRetract:
        {
            data.forEachValue([&](const auto & key, auto & mapped) {
                if (!TrackingUpdatesWithRetract::updated(mapped))
                    return;

                /// FIXME: If the key group is empty, also remove the key
                if (!TrackingUpdatesWithRetract::empty(mapped))
                    callback(key, mapped);

                TrackingUpdatesWithRetract::resetUpdated(mapped);
            });
            break;
        }
        case AggregatingConvertType::Retract:
        {
            data.forEachValue([&](const auto & key, auto & mapped) {
                if (!TrackingUpdatesWithRetract::hasRetract(mapped))
                    return;

                callback(key, TrackingUpdatesWithRetract::getRetract(mapped));
            });
            break;
        }
    }
}

template <bool return_single_block, typename Method, typename Table>
MemoryAggregator::ConvertToBlockRes<return_single_block> NO_INLINE MemoryAggregator::convertToBlockImplFinal(
    Method & method,
    Table & data,
    const ArenaPtr & pool,
    size_t,
    AggregatingConvertParams & cparams,
    bool use_compiled_functions [[maybe_unused]]) const
{
    /// +1 for nullKeyData, if `data` doesn't have it - not a problem, just some memory for one excessive row will be preallocated
    const size_t max_block_size = (return_single_block ? data.size() : std::min(params->max_block_size, data.size())) + 1;
    ConvertToBlockRes<return_single_block> res;
    Arena * arena = pool.get();

    OutputBlockColumns out_cols;
    std::optional<Sizes> shuffled_key_sizes;
    PaddedPODArray<AggregateDataPtr> places, skipped_places;

    SCOPE_EXIT({
        /// Retract data is no longer needed after converted
        if (cparams.type == AggregatingConvertType::Retract)
        {
            for (auto & place : places)
                destroyAggregateStates(place);

            for (auto & place : skipped_places)
                destroyAggregateStates(place);
        }
    });

    auto init_out_cols = [&]() {
        out_cols = prepareOutputBlockColumns(getHeader(), pool, max_block_size);

        if constexpr (Method::low_cardinality_optimization)
        {
            if (data.hasNullKeyData())
            {
                chassert(cparams.type == AggregatingConvertType::Normal);
                out_cols.key_columns[0]->insertDefault();
                insertAggregatesIntoColumns(data.getNullKeyData(), out_cols.final_aggregate_columns, arena);
                data.hasNullKeyData() = false;
            }
        }

        shuffled_key_sizes = method.shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);

        places.clear();
        places.reserve(max_block_size);
    };

    /// Should be invoked at least once, because null data might be the only content of the `data`
    init_out_cols();

    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

    if (params->group_by != IAggregatorParams::GroupBy::UserDefined)
    {
        if (cparams.type != AggregatingConvertType::Retract)
        {
            convertState<Method>(data, cparams, [&](const auto & key, auto & mapped) {
                if constexpr (!return_single_block)
                {
                    /// If reached max block size, finalize the block and start a new one
                    if (out_cols.key_columns[0]->size() >= max_block_size)
                    {
                        res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), arena, use_compiled_functions));
                        init_out_cols();
                    }
                }

                method.insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);
                places.emplace_back(mapped);
            });
        }
        else
        {
            convertState<Method>(data, cparams, [&](const auto & key, auto & mapped) {
                if constexpr (!return_single_block)
                {
                    /// If reached max block size, finalize the block and start a new one
                    if (out_cols.key_columns[0]->size() >= max_block_size)
                    {
                        res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), arena, use_compiled_functions));
                        /// Retract data is no longer needed after converted
                        for (auto & place : places)
                            destroyAggregateStates(place);

                        init_out_cols();
                    }
                }

                if (!TrackingUpdatesWithRetract::empty(mapped)) [[likely]]
                {
                    method.insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);
                    places.emplace_back(mapped);
                }
                else
                {
                    /// If retraction is empty, skip the output
                    skipped_places.emplace_back(mapped);
                }
                mapped = nullptr; /// Retract data will be destroyed later
            });
        }
    }
    else
    {
        /// For UDA with own emit strategy, there are two special cases to be handled:
        /// 1. not all groups need to be emitted. therefore proton needs to pick groups
        /// that should emits, and only emit those groups while keep other groups unchanged.
        /// 2. a single block trigger multiple emits. In this case, proton need insert the
        /// same key multiple times for each emit result of this group.
        convertState<Method>(data, cparams, [&](const auto & key, auto & mapped) {
            if constexpr (!return_single_block)
            {
                /// If reached max block size, finalize the block and start a new one
                if (out_cols.key_columns[0]->size() >= max_block_size)
                {
                    res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), arena, use_compiled_functions));
                    init_out_cols();
                }
            }

            /// for non-UDA or UDA without emit strategy, 'should_emit' is always true.
            /// For UDA with emit strategy, it is true only if the group should emit.
            chassert(aggregate_functions.size() == 1);
            size_t emit_times = aggregate_functions[0]->getEmitTimes(mapped + offsets_of_aggregate_states[0]);
            if (emit_times > 0)
            {
                /// duplicate key for each emit
                for (size_t i = 0; i < emit_times; i++)
                    method.insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);

                places.emplace_back(mapped);
            }
        });
    }

    if (places.empty())
        return {};

    if constexpr (return_single_block)
    {
        return insertResultsIntoColumns(places, std::move(out_cols), arena, use_compiled_functions);
    }
    else
    {
        res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), arena, use_compiled_functions));
        return res;
    }
}

BlocksList
MemoryAggregator::prepareBlockAndFillSingleLevel(MemoryAggregatedDataVariants & data_variants, AggregatingConvertParams & cparams) const
{
    const size_t rows = data_variants.sizeWithoutOverflowRow();
    constexpr bool return_single_block = false;
    auto arena = cparams.type == AggregatingConvertType::Retract ? data_variants.retract_pool : data_variants.aggregates_pool;

#define M(NAME) \
    else if (data_variants.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        return convertToBlockImpl<return_single_block>(*data_variants.NAME, data_variants.NAME->data, arena, rows, cparams); \
    }

    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }
}

template <typename Method>
void NO_INLINE MemoryAggregator::mergeSingleLevelDataImpl(ManyMemoryAggregatedDataVariants & non_empty_data) const
{
    MemoryAggregatedDataVariantsPtr & res = non_empty_data[0];

    bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
    if (compiled_aggregate_functions_holder)
        use_compiled_functions = true;
#endif

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        if (!checkLimits(res->sizeWithoutOverflowRow()))
            break;

        MemoryAggregatedDataVariants & current = *non_empty_data[result_num];

        mergeDataImpl<Method>(
            getDataVariant<Method>(*res).data, getDataVariant<Method>(current).data, res->aggregates_pool, use_compiled_functions);
    }
}

#define M(NAME) \
    template void NO_INLINE MemoryAggregator::mergeSingleLevelDataImpl<decltype(MemoryAggregatedDataVariants::NAME)::element_type>( \
        ManyMemoryAggregatedDataVariants & non_empty_data) const;
APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M

/// == Two level

BlocksList
MemoryAggregator::mergeAndConvertTwoLevelToBlocks(ManyMemoryAggregatedDataVariantsPtr prepared_data_ptr, size_t max_threads) const
{
    auto total_size = std::accumulate(prepared_data_ptr->begin(), prepared_data_ptr->end(), 0ull, [](size_t size, const auto & variants) {
        return size + variants->sizeWithoutOverflowRow();
    });
    /// TODO Make a custom threshold.
    /// TODO Use the shared thread pool with the `merge` function.
    std::unique_ptr<ThreadPool> thread_pool;
    if (max_threads > 1 && total_size > 100000)
        thread_pool = std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, max_threads);

    auto & first = *prepared_data_ptr->at(0);
    if (false)
    {
    } // NOLINT
#define M(NAME) \
    else if (first.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        return mergeAndConvertTwoLevelToBlocksImpl<decltype(first.NAME)::element_type>(*prepared_data_ptr, thread_pool.get()); \
    }

    APPLY_FOR_VARIANTS_ALL_TWO_LEVEL(M)
#undef M
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }
}

template <typename Method>
Block MemoryAggregator::convertOneBucketToBlockImpl(
    Method & method, const ArenaPtr & pool, Int64 bucket, AggregatingConvertParams & cparams) const
{
    bool convert_updates = cparams.type == AggregatingConvertType::Updates || cparams.type == AggregatingConvertType::UpdatesAfterRetract;
    if (convert_updates && !method.data.isBucketUpdated(bucket))
        return {};

    constexpr bool return_single_block = true;
    Block block
        = convertToBlockImpl<return_single_block>(method, method.data.impls[bucket], pool, method.data.impls[bucket].size(), cparams);
    block.info.bucket_num = static_cast<int>(bucket);
    if (convert_updates)
        method.data.resetUpdatedBucket(bucket); /// finalized
    return block;
}

BlocksList MemoryAggregator::prepareBlocksAndFillTwoLevel(
    MemoryAggregatedDataVariants & data_variants, size_t max_threads, AggregatingConvertParams & cparams) const
{
    /// TODO Make a custom threshold.
    /// TODO Use the shared thread pool with the `merge` function.
    std::unique_ptr<ThreadPool> thread_pool;
    if (max_threads > 1 && data_variants.sizeWithoutOverflowRow() > 100000
        && cparams.type == AggregatingConvertType::Normal) /// use single thread for non-final or retracted data or updated data
        thread_pool = std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, max_threads);

    if (false)
    {
    } // NOLINT
#define M(NAME) \
    else if (data_variants.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        return prepareBlocksAndFillTwoLevelImpl(data_variants, *data_variants.NAME, thread_pool.get(), cparams); \
    }

    APPLY_FOR_VARIANTS_ALL_TWO_LEVEL(M)
#undef M
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }
}

template <typename Method>
BlocksList MemoryAggregator::prepareBlocksAndFillTwoLevelImpl(
    MemoryAggregatedDataVariants & data_variants, Method & method, ThreadPool * thread_pool, AggregatingConvertParams & cparams) const
{
    return convertBucketsInParallel(thread_pool, method.data.buckets(), [&](Int64 bucket) -> BlocksList {
        /// For time bucket two-level hash table, there are two cases:
        /// 1) If it's a source variants, the state is stored in the time bucket arena, which is used for time window recycling.
        /// 2) If it's a merged variants for the output, we only use the global arena.
        const auto & arena = (data_variants.isTimeBucketTwoLevel() && !data_variants.time_bucket_arenas.empty())
            ? data_variants.time_bucket_arenas.at(bucket)
            : data_variants.aggregates_pool;

        return {convertOneBucketToBlockImpl(method, arena, bucket, cparams)};
    });
}

bool MemoryAggregator::checkAndProcessResult(MemoryAggregatedDataVariants & result) const
{
    size_t result_size = result.sizeWithoutOverflowRow();

    /// NOTE: Do not use the query memory usage here: it reflects the whole query (buffers, inputs, joins, etc.)
    /// and may spike for reasons unrelated to this aggregator, causing accidental two‑level conversion.
    /// We could also count bytes of this variants, but row count is usually sufficient.
    bool worth_convert_to_two_level = worthConvertToTwoLevel(
        memory_params->group_by_two_level_threshold, result_size, /*group_by_two_level_threshold_bytes=*/0, /*result_size_bytes=*/0);

    /** Converting to a two-level data structure.
      * It allows you to make, in the subsequent, an effective merge - either economical from memory or parallel.
      */
    if (result.isConvertibleToTwoLevel() && worth_convert_to_two_level)
        result.convertToTwoLevel();

    /// Checking the constraints.
    if (!checkLimits(result_size))
        return true;

    return false;
}

template <typename Method>
BlocksList
MemoryAggregator::mergeAndConvertTwoLevelToBlocksImpl(ManyMemoryAggregatedDataVariants & non_empty_data, ThreadPool * thread_pool) const
{
    auto & first = *non_empty_data.at(0);

    std::vector<Int64> buckets;
    if (first.isStaticBucketTwoLevel())
    {
        buckets = getDataVariant<Method>(first).data.buckets();
    }
    else
    {
        chassert(first.isTimeBucketTwoLevel());
        std::unordered_set<Int64> buckets_set;
        for (auto & data_variants : non_empty_data)
        {
            auto tmp_buckets = getDataVariant<Method>(*data_variants).data.buckets();
            buckets_set.insert(tmp_buckets.begin(), tmp_buckets.end());
        }
        buckets.assign(buckets_set.begin(), buckets_set.end());
    }

    return convertBucketsInParallel(thread_pool, buckets, [&](Int64 bucket) -> BlocksList {
        /// Merge all buckets into the first one and use its owned pool instead of external temporary pool for converting.
        mergeBucketImpl<Method>(non_empty_data, bucket, first.aggregates_pool);
        AggregatingConvertParams cparams{AggregatingConvertType::Normal};
        return {convertOneBucketToBlockImpl(getDataVariant<Method>(first), first.aggregates_pool, bucket, cparams)};
    });
}

template <typename Method>
void NO_INLINE MemoryAggregator::mergeBucketImpl(
    ManyMemoryAggregatedDataVariants & data, Int64 bucket, const ArenaPtr & pool, std::atomic<bool> * is_cancelled) const
{
    bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
    if (compiled_aggregate_functions_holder)
        use_compiled_functions = true;
#endif

    /// We merge all aggregation results to the first.
    MemoryAggregatedDataVariantsPtr & res = data[0];
    for (size_t result_num = 1, size = data.size(); result_num < size; ++result_num)
    {
        if (is_cancelled && is_cancelled->load(std::memory_order_seq_cst))
            return;

        MemoryAggregatedDataVariants & current = *data[result_num];
        mergeDataImpl<Method>(
            getDataVariant<Method>(*res).data.impls[bucket],
            getDataVariant<Method>(current).data.impls[bucket],
            pool,
            use_compiled_functions);

        /// Assume the current bucket has been finalized.
        getDataVariant<Method>(current).data.resetUpdatedBucket(bucket);
    }
}
}
}
