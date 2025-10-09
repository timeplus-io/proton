#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/CurrentMetrics.h>
#include <Common/scope_guard_safe.h>

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
        Arena arena;
        while (true)
        {
            if (cancelled && cancelled->test())
                break;

            UInt32 bucket_idx = next_bucket_idx_to_merge.fetch_add(1);
            if (bucket_idx >= buckets.size())
                break;

            auto bucket = buckets[bucket_idx];
            blocks.splice(blocks.end(), bucket_converter(bucket, &arena));
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
            auto retract_blocks = doConvertToBlocks(data_variants, /*final_=*/true, max_threads, retract_cparams);
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

            auto updated_blocks = doConvertToBlocks(data_variants, /*final_=*/true, max_threads, update_cparams);
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

    return doConvertToBlocks(data_variants, /*final_=*/true, max_threads, cparams);
}

BlocksList MemoryAggregator::doConvertToBlocks(
    IAggregatedDataVariants & variants, bool final_, size_t max_threads, AggregatingConvertParams & cparams) const
{
    chassert(variants.aggregatorType() == AggregatorType::Memory);
    auto & data_variants = static_cast<MemoryAggregatedDataVariants &>(variants);

    LOG_DEBUG(logger, "Converting aggregated {} data to blocks", magic_enum::enum_name(cparams.type));

    Stopwatch watch;

    BlocksList blocks;

    /// In what data structure is the data aggregated?
    if (data_variants.empty())
        return blocks;

    if (cparams.type == AggregatingConvertType::Updates || cparams.type == AggregatingConvertType::UpdatesAfterRetract)
    {
        chassert(final_);
        max_threads = 1;
    }
    else if (cparams.type == AggregatingConvertType::Retract)
    {
        chassert(final_);
        max_threads = 1;
    }

    if (data_variants.type == MemoryAggregatedDataVariants::Type::without_key)
        blocks.emplace_back(prepareBlockAndFillWithoutKey(data_variants, final_, cparams));
    else if (!data_variants.isTwoLevel())
        blocks = prepareBlockAndFillSingleLevel(data_variants, final_, cparams);
    else
        blocks = prepareBlocksAndFillTwoLevel(data_variants, final_, max_threads, cparams);

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
            return doConvertToBlocks(*merged_updated_data, /*final_=*/true, max_threads, merged_cparams);
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
                AggregatingConvertParams merged_cparams{AggregatingConvertType::Normal};
                auto retract_blocks = doConvertToBlocks(*merged_retracted_data, /*final_=*/true, max_threads, merged_cparams);
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

        /// Then collect updated aggregate states to blocks
        {
            AggregatingConvertParams update_cparams{
                AggregatingConvertType::UpdatesAfterRetract, std::move(cparams.many_updated_keys), cparams.keys_already_sharded};
            auto merged_updated_data = mergeGroups(many_data_variants, update_cparams);
            if (merged_updated_data)
            {
                AggregatingConvertParams merged_cparams{AggregatingConvertType::Normal};
                auto updated_blocks = doConvertToBlocks(*merged_updated_data, /*final_=*/true, max_threads, merged_cparams);
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

    auto prepared_data_ptr = prepareVariantsToMerge(many_data_variants);
    if (prepared_data_ptr->empty())
        return {};

    SCOPE_EXIT({
        bool clear_states = cparams.type == AggregatingConvertType::Normal && cparams.clear_state;
        if (clear_states)
        {
            for (auto & variants : *prepared_data_ptr)
                variants->reset();
        }
    });

    BlocksList blocks;
    auto & first = *prepared_data_ptr->at(0);
    if (first.type == MemoryAggregatedDataVariants::Type::without_key)
    {
        mergeWithoutKeyDataImpl(*prepared_data_ptr);
        blocks.emplace_back(prepareBlockAndFillWithoutKey(first, /*final_=*/true, cparams));
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

        blocks = prepareBlockAndFillSingleLevel(first, /*final_=*/true, cparams);
    }
    else
    {
        return mergeAndConvertTwoLevelToBlocks(prepared_data_ptr, /*final_=*/true, max_threads);
    }

    return blocks;
}

template <bool return_single_block, typename Method, typename Table>
MemoryAggregator::ConvertToBlockRes<return_single_block> MemoryAggregator::convertToBlockImpl(
    Method & method, Table & data, Arena * arena, Arenas & aggregates_pools, bool final, size_t rows, AggregatingConvertParams & cparams)
    const
{
    if (data.empty())
    {
        auto && out_cols = prepareOutputBlockColumns(getHeader(final), aggregates_pools, final, rows);
        return {finalizeBlock(getHeader(final), std::move(out_cols), final, rows)};
    }

    ConvertToBlockRes<return_single_block> res;
    bool use_compiled_functions = false;
    if (final)
    {
#if USE_EMBEDDED_COMPILER
        use_compiled_functions = compiled_aggregate_functions_holder != nullptr && !Method::low_cardinality_optimization;
#endif
        res = convertToBlockImplFinal<return_single_block>(method, data, arena, aggregates_pools, rows, cparams, use_compiled_functions);
    }
    else
    {
        chassert(cparams.type == AggregatingConvertType::Normal);
        res = convertToBlockImplNotFinal<return_single_block>(method, data, aggregates_pools, rows);
    }

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
    Arena * arena,
    Arenas & aggregates_pools,
    size_t,
    AggregatingConvertParams & cparams,
    bool use_compiled_functions [[maybe_unused]]) const

{
    /// +1 for nullKeyData, if `data` doesn't have it - not a problem, just some memory for one excessive row will be preallocated
    const size_t max_block_size = (return_single_block ? data.size() : std::min(params->max_block_size, data.size())) + 1;
    constexpr bool final = true;
    ConvertToBlockRes<return_single_block> res;

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
        out_cols = prepareOutputBlockColumns(getHeader(final), aggregates_pools, final, max_block_size);

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

template <bool return_single_block, typename Method, typename Table>
MemoryAggregator::ConvertToBlockRes<return_single_block>
    NO_INLINE MemoryAggregator::convertToBlockImplNotFinal(Method & method, Table & data, Arenas & aggregates_pools, size_t) const
{
    /// +1 for nullKeyData, if `data` doesn't have it - not a problem, just some memory for one excessive row will be preallocated
    const size_t max_block_size = (return_single_block ? data.size() : std::min(params->max_block_size, data.size())) + 1;
    constexpr bool final = false;
    ConvertToBlockRes<return_single_block> res;

    std::optional<OutputBlockColumns> out_cols;
    std::optional<Sizes> shuffled_key_sizes;
    size_t rows_in_current_block = 0;

    auto init_out_cols = [&]() {
        out_cols = prepareOutputBlockColumns(getHeader(final), aggregates_pools, final, max_block_size);

        if constexpr (Method::low_cardinality_optimization)
        {
            if (data.hasNullKeyData())
            {
                out_cols->raw_key_columns[0]->insertDefault();

                for (size_t i = 0; i < params->aggregates_size; ++i)
                    out_cols->aggregate_columns_data[i]->push_back(data.getNullKeyData() + offsets_of_aggregate_states[i]);

                ++rows_in_current_block;
                data.getNullKeyData() = nullptr;
                data.hasNullKeyData() = false;
            }
        }

        shuffled_key_sizes = method.shuffleKeyColumns(out_cols->raw_key_columns, key_sizes);
    };

    // should be invoked at least once, because null data might be the only content of the `data`
    init_out_cols();

    data.forEachValue([&](const auto & key, auto & mapped) {
        if (!out_cols.has_value())
            init_out_cols();

        const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;
        method.insertKeyIntoColumns(key, out_cols->raw_key_columns, key_sizes_ref);

        /// reserved, so push_back does not throw exceptions
        for (size_t i = 0; i < params->aggregates_size; ++i)
            out_cols->aggregate_columns_data[i]->push_back(mapped + offsets_of_aggregate_states[i]);

        ++rows_in_current_block;

        if constexpr (!return_single_block)
        {
            if (rows_in_current_block >= max_block_size)
            {
                res.emplace_back(finalizeBlock(getHeader(final), std::move(out_cols.value()), final, rows_in_current_block));
                out_cols.reset();
                rows_in_current_block = 0;
            }
        }
    });

    if constexpr (return_single_block)
    {
        return finalizeBlock(getHeader(final), std::move(out_cols).value(), final, rows_in_current_block);
    }
    else
    {
        if (rows_in_current_block)
            res.emplace_back(finalizeBlock(getHeader(final), std::move(out_cols).value(), final, rows_in_current_block));
        return res;
    }
    return res;
}

BlocksList MemoryAggregator::prepareBlockAndFillSingleLevel(
    MemoryAggregatedDataVariants & data_variants, bool final, AggregatingConvertParams & cparams) const
{
    const size_t rows = data_variants.sizeWithoutOverflowRow();
    constexpr bool return_single_block = false;
#define M(NAME) \
    else if (data_variants.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        return convertToBlockImpl<return_single_block>( \
            *data_variants.NAME, \
            data_variants.NAME->data, \
            data_variants.aggregates_pool, \
            data_variants.aggregates_pools, \
            final, \
            rows, \
            cparams); \
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

BlocksList MemoryAggregator::mergeAndConvertTwoLevelToBlocks(
    ManyMemoryAggregatedDataVariantsPtr prepared_data_ptr, bool final, size_t max_threads) const
{
    auto total_size = std::accumulate(prepared_data_ptr->begin(), prepared_data_ptr->end(), 0ull, [](size_t size, const auto & variants) {
        return size + variants->sizeWithoutOverflowRow();
    });
    /// TODO Make a custom threshold.
    /// TODO Use the shared thread pool with the `merge` function.
    std::unique_ptr<ThreadPool> thread_pool;
    if (max_threads > 1 && total_size > 100000 && final)
        thread_pool = std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, max_threads);

    auto & first = *prepared_data_ptr->at(0);
    if (false)
    {
    } // NOLINT
#define M(NAME) \
    else if (first.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        return mergeAndConvertTwoLevelToBlocksImpl<decltype(first.NAME)::element_type>(*prepared_data_ptr, final, thread_pool.get()); \
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
    MemoryAggregatedDataVariants & data_variants,
    Method & method,
    Arena * arena,
    bool final,
    Int64 bucket,
    AggregatingConvertParams & cparams) const
{
    bool convert_updates = cparams.type == AggregatingConvertType::Updates || cparams.type == AggregatingConvertType::UpdatesAfterRetract;
    if (convert_updates && !method.data.isBucketUpdated(bucket))
        return {};

    constexpr bool return_single_block = true;
    Block block = convertToBlockImpl<return_single_block>(
        method, method.data.impls[bucket], arena, data_variants.aggregates_pools, final, method.data.impls[bucket].size(), cparams);
    block.info.bucket_num = static_cast<int>(bucket);
    if (convert_updates)
        method.data.resetUpdatedBucket(bucket); /// finalized
    return block;
}

BlocksList MemoryAggregator::prepareBlocksAndFillTwoLevel(
    MemoryAggregatedDataVariants & data_variants, bool final, size_t max_threads, AggregatingConvertParams & cparams) const
{
    /// TODO Make a custom threshold.
    /// TODO Use the shared thread pool with the `merge` function.
    std::unique_ptr<ThreadPool> thread_pool;
    if (max_threads > 1 && data_variants.sizeWithoutOverflowRow() > 100000 && final
        && cparams.type == AggregatingConvertType::Normal) /// use single thread for non-final or retracted data or updated data
        thread_pool = std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, max_threads);

    if (false)
    {
    } // NOLINT
#define M(NAME) \
    else if (data_variants.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        return prepareBlocksAndFillTwoLevelImpl(data_variants, *data_variants.NAME, final, thread_pool.get(), cparams); \
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
    MemoryAggregatedDataVariants & data_variants, Method & method, bool final, ThreadPool * thread_pool, AggregatingConvertParams & cparams)
    const
{
    return convertBucketsInParallel(thread_pool, method.data.buckets(), [&](Int64 bucket, Arena * arena) -> BlocksList {
        return {convertOneBucketToBlockImpl(data_variants, method, arena, final, bucket, cparams)};
    });
}

bool MemoryAggregator::checkAndProcessResult(MemoryAggregatedDataVariants & result) const
{
    size_t result_size = result.sizeWithoutOverflowRow();
    Int64 current_memory_usage = 0;
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            current_memory_usage = memory_tracker->get();

    /// Here all the results in the sum are taken into account, from different threads.
    Int64 result_size_bytes = current_memory_usage - memory_usage_before_aggregation;

    bool worth_convert_to_two_level = worthConvertToTwoLevel(
        memory_params->group_by_two_level_threshold, result_size, memory_params->group_by_two_level_threshold_bytes, result_size_bytes);

    /** Converting to a two-level data structure.
      * It allows you to make, in the subsequent, an effective merge - either economical from memory or parallel.
      */
    if (result.isConvertibleToTwoLevel() && worth_convert_to_two_level)
        result.convertToTwoLevel();

    /// Checking the constraints.
    if (!checkLimits(result_size))
        return true;

    /** Flush data to disk if too much RAM is consumed.
      * Data can only be flushed to disk if a two-level aggregation structure is used.
      */
    if (memory_params->max_bytes_before_external_group_by && result.isTwoLevel()
        && current_memory_usage > static_cast<Int64>(memory_params->max_bytes_before_external_group_by) && worth_convert_to_two_level)
    {
        size_t size = current_memory_usage + memory_params->min_free_disk_space;
        writeToTemporaryFile(result, size);
    }

    return false;
}

template <typename Method>
BlocksList MemoryAggregator::mergeAndConvertTwoLevelToBlocksImpl(
    ManyMemoryAggregatedDataVariants & non_empty_data, bool final, ThreadPool * thread_pool) const
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

    return convertBucketsInParallel(thread_pool, buckets, [&](Int64 bucket, Arena * arena) -> BlocksList {
        /// Merge all buckets into the first one and use its owned pool instead of external temporary pool for converting.
        mergeBucketImpl<Method>(non_empty_data, bucket, first.aggregates_pool);
        AggregatingConvertParams cparams{AggregatingConvertType::Normal};
        return {convertOneBucketToBlockImpl(first, getDataVariant<Method>(first), arena, final, bucket, cparams)};
    });
}

template <typename Method>
void NO_INLINE MemoryAggregator::mergeBucketImpl(
    ManyMemoryAggregatedDataVariants & data, Int64 bucket, Arena * arena, std::atomic<bool> * is_cancelled) const
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
            arena,
            use_compiled_functions);

        /// Assume the current bucket has been finalized.
        getDataVariant<Method>(current).data.resetUpdatedBucket(bucket);
    }
}

void MemoryAggregator::writeToTemporaryFile(MemoryAggregatedDataVariants & data_variants, size_t max_temp_file_size) const
{
    if (!tmp_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write to temporary file because temporary file is not initialized");

    Stopwatch watch;
    size_t rows = data_variants.size();

    auto & out_stream = tmp_data->createStream(getHeader(false), max_temp_file_size);
    ProfileEvents::increment(ProfileEvents::ExternalAggregationWritePart);

    LOG_DEBUG(logger, "Writing part of aggregation data into temporary file {}", out_stream.getPath());

    /// Flush only two-level data and possibly overflow data.

#define M(NAME) \
    else if (data_variants.type == MemoryAggregatedDataVariants::Type::NAME) \
        writeToTemporaryFileImpl(data_variants, *data_variants.NAME, out_stream);

    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_ALL_TWO_LEVEL(M)
#undef M
    else throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant");

    /// NOTE Instead of freeing up memory and creating new hash tables and arenas, you can re-use the old ones.
    data_variants.init(data_variants.type);
    data_variants.aggregates_pools = Arenas(1, std::make_shared<Arena>());
    data_variants.aggregates_pool = data_variants.aggregates_pools.back().get();
    initStatesForWithoutKey(data_variants);

    auto stat = out_stream.finishWriting();

    ProfileEvents::increment(ProfileEvents::ExternalAggregationCompressedBytes, stat.compressed_size);
    ProfileEvents::increment(ProfileEvents::ExternalAggregationUncompressedBytes, stat.uncompressed_size);
    ProfileEvents::increment(ProfileEvents::ExternalProcessingCompressedBytesTotal, stat.compressed_size);
    ProfileEvents::increment(ProfileEvents::ExternalProcessingUncompressedBytesTotal, stat.uncompressed_size);

    double elapsed_seconds = watch.elapsedSeconds();
    double compressed_size = stat.compressed_size;
    double uncompressed_size = stat.uncompressed_size;
    LOG_DEBUG(
        logger,
        "Written part in {:.3f} sec., {} rows, {} uncompressed, {} compressed,"
        " {:.3f} uncompressed bytes per row, {:.3f} compressed bytes per row, compression rate: {:.3f}"
        " ({:.3f} rows/sec., {}/sec. uncompressed, {}/sec. compressed)",
        elapsed_seconds,
        rows,
        ReadableSize(uncompressed_size),
        ReadableSize(compressed_size),
        static_cast<double>(uncompressed_size) / rows,
        static_cast<double>(compressed_size) / rows,
        static_cast<double>(uncompressed_size) / compressed_size,
        static_cast<double>(rows) / elapsed_seconds,
        ReadableSize(static_cast<double>(uncompressed_size) / elapsed_seconds),
        ReadableSize(static_cast<double>(compressed_size) / elapsed_seconds));
}

template <typename Method>
void MemoryAggregator::writeToTemporaryFileImpl(
    MemoryAggregatedDataVariants & data_variants, Method & method, TemporaryFileStream & out) const
{
    size_t max_temporary_block_size_rows = 0;
    size_t max_temporary_block_size_bytes = 0;

    auto update_max_sizes = [&](const Block & block) {
        size_t block_size_rows = block.rows();
        size_t block_size_bytes = block.bytes();

        if (block_size_rows > max_temporary_block_size_rows)
            max_temporary_block_size_rows = block_size_rows;
        if (block_size_bytes > max_temporary_block_size_bytes)
            max_temporary_block_size_bytes = block_size_bytes;
    };

    AggregatingConvertParams cparams{AggregatingConvertType::Normal};
    for (auto bucket : method.data.buckets())
    {
        Block block = convertOneBucketToBlockImpl(data_variants, method, data_variants.aggregates_pool, false, bucket, cparams);
        out.write(block);
        update_max_sizes(block);
    }

    /// Pass ownership of the aggregate functions states:
    /// `data_variants` will not destroy them in the destructor, they are now owned by ColumnAggregateFunction objects.
    data_variants.aggregator = nullptr;

    LOG_DEBUG(
        logger, "Max size of temporary block: {} rows, {}.", max_temporary_block_size_rows, ReadableSize(max_temporary_block_size_bytes));
}

}

}
