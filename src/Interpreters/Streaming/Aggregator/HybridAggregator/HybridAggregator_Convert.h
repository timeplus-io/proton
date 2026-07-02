#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingCount.h>
#include <Common/HybridHashTable/HybridKeyGetter.h>
#include <Common/formatReadable.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{

BlocksList
HybridAggregator::convertToBlocks(IAggregatedDataVariants & variants, size_t /*max_threads*/, AggregatingConvertParams & cparams) const
{
    BlocksList blocks;

    Stopwatch watch;
    size_t rows = 0;
    size_t bytes = 0;

    LOG_DEBUG(logger, "Converting aggregated {} data to blocks", magic_enum::enum_name(params->tracking_updates_type));

    chassert(variants.aggregatorType() == AggregatorType::Hybrid);
    auto & data_variants = static_cast<HybridAggregatedDataVariants &>(variants);
    if (data_variants.empty())
        return {};

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

    switch (method_chosen)
    {
        case HybridHashType::Empty:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "HybridHashTable is not inited");
        }
        case HybridHashType::key_hashed:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Hashed key shall not be chosen for aggregation");
        }
        case HybridHashType::WithoutKey:
        {
            blocks = convertToBlocksWithoutKey(data_variants);
            break;
        }

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        if (has_nullable_key) \
        { \
            using KeyGetter = HybridKeyGetter<HybridHashType::NAME, /*nullable=*/true>; \
            blocks = convertToBlocksImpl<KeyGetter>( \
                *data_variants.table.NAME, data_variants.updates.NAME.get(), data_variants.retracts.NAME.get(), /*clear_updates=*/true); \
        } \
        else \
        { \
            using KeyGetter = HybridKeyGetter<HybridHashType::NAME, /*nullable=*/false>; \
            blocks = convertToBlocksImpl<KeyGetter>( \
                *data_variants.table.NAME, data_variants.updates.NAME.get(), data_variants.retracts.NAME.get(), /*clear_updates=*/true); \
        } \
        break; \
    }
            APPLY_FOR_HASH_KEY_VARIANTS_HYBRID(M)
#undef M
    }

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

template <typename KeyGetter, typename Table>
BlocksList HybridAggregator::convertToBlocksMerged(Table & table, Table * retracts) const
{
    if (params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract)
    {
        auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);
        auto retract_blocks = convertToBlocksForAll<KeyGetter>(*retracts);
        for (auto & block : retract_blocks)
        {
            auto retract_delta_col = ColumnInt8::create(block.rows(), static_cast<Int8>(-1));
            block.insert(ColumnWithTypeAndName{std::move(retract_delta_col), delta_col_type, "_tp_delta"});
        }

        auto blocks = convertToBlocksForAll<KeyGetter>(table);
        for (auto & block : blocks)
        {
            auto delta_col = ColumnInt8::create(block.rows(), static_cast<Int8>(1));
            block.insert(ColumnWithTypeAndName{std::move(delta_col), delta_col_type, "_tp_delta"});
        }

        retract_blocks.splice(retract_blocks.end(), std::move(blocks));
        return retract_blocks;
    }
    else
    {
        return convertToBlocksForAll<KeyGetter>(table);
    }
}

template <typename KeyGetter, typename Table>
ALWAYS_INLINE BlocksList HybridAggregator::convertToBlocksImpl(Table & table, Table * updates, Table * retracts, bool clear_updates) const
{
    switch (params->tracking_updates_type)
    {
        case TrackingUpdatesType::Updates:
            return convertToBlocksForUpdates<KeyGetter>(table, updates, clear_updates);
        case TrackingUpdatesType::UpdatesWithRetract:
            return convertToBlocksForRetracts<KeyGetter>(table, retracts);
        case TrackingUpdatesType::None:
            return convertToBlocksForAll<KeyGetter>(table);
    }
}

template <typename KeyGetter, typename Table>
BlocksList HybridAggregator::convertToBlocksForAll(Table & table) const
{
    auto rows = table.approximateCount();
    /// +1 for nullKeyData, if `data` doesn't have it - not a problem, just some memory for one excessive row will be preallocated
    const size_t max_block_size = std::min(params->max_block_size, rows) + 1;

    OutputBlockColumns out_cols;
    std::optional<Sizes> shuffled_key_sizes;
    PaddedPODArray<ConstAggregateDataPtr> places;

    auto init_out_cols = [&]() {
        out_cols = prepareOutputBlockColumns(getHeader(), /*aggregates_pool=*/nullptr, max_block_size);
        shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);

        places.clear();
        places.reserve(max_block_size);
    };

    /// should be invoked at least once, because null data might be the only content of the `data`
    init_out_cols();

    BlocksList res;

    auto done_callback = [&]() {
        if (!places.empty())
            res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), /*arena=*/nullptr));
    };

    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

    if (params->group_by != IAggregatorParams::GroupBy::UserDefined)
    {
        if (trackingStateCount())
        {
            table.forBatchValue(
                std::min(max_block_size, table.getConfig().base_conf.max_hot_key_count),
                [&](const KeyGetter::KeyType & key, auto value, bool flush) {
                    auto mapped = static_cast<ConstAggregateDataPtr>(value.getMapped());
                    if (!TrackingCount::empty(mapped))
                    {
                        KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);
                        places.emplace_back(mapped);
                    }

                    /// If reached max block size, finalize the block and start a new one
                    if (flush)
                    {
                        res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), /*arena=*/nullptr));
                        init_out_cols();
                    }
                },
                done_callback);
        }
        else
        {
            table.forBatchValue(
                std::min(max_block_size, table.getConfig().base_conf.max_hot_key_count),
                [&](const KeyGetter::KeyType & key, auto value, bool flush) {
                    KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);
                    places.emplace_back(static_cast<ConstAggregateDataPtr>(value.getMapped()));

                    /// If reached max block size, finalize the block and start a new one
                    if (flush)
                    {
                        res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), /*arena=*/nullptr));
                        init_out_cols();
                    }
                },
                done_callback);
        }
    }
    else
    {
        /// For UDA with own emit strategy, there are two special cases to be handled:
        /// 1. not all groups need to be emitted. therefore proton needs to pick groups
        /// that should emits, and only emit those groups while keep other groups unchanged.
        /// 2. a single block trigger multiple emits. In this case, proton need insert the
        /// same key multiple times for each emit result of this group.
        table.forBatchValue(
            max_block_size,
            [&](const KeyGetter::KeyType & key, auto value, bool flush) {
                const auto * mapped = static_cast<ConstAggregateDataPtr>(value.getMapped());
                /// for non-UDA or UDA without emit strategy, 'should_emit' is always true.
                /// For UDA with emit strategy, it is true only if the group should emit.
                assert(aggregate_functions.size() == 1);
                size_t emit_times = aggregate_functions[0]->getEmitTimes(mapped + offsets_of_aggregate_states[0]);
                if (emit_times > 0)
                {
                    /// Duplicate key for each emit
                    for (size_t i = 0; i < emit_times; i++)
                        KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);

                    places.emplace_back(mapped);
                }

                /// If reached max block size, finalize the block and start a new one
                if (flush)
                {
                    res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), /*arena=*/nullptr));
                    init_out_cols();
                }
            },
            done_callback);
    }

    return res;
}

/// \param clear_updates if it is true, clear up table which tracks updates
/// Only for hopping GCD windows, clear_updates is temporary false and will clear manually after watermark progresses
template <typename KeyGetter, typename Table>
BlocksList HybridAggregator::convertToBlocksForUpdates(Table & table, Table * updates, bool clear_updates) const
{
    assert(updates);

    auto rows = table.approximateCount();

    /// +1 for nullKeyData, if `data` doesn't have it - not a problem, just some memory for one excessive row will be preallocated
    const size_t max_block_size = std::min(params->max_block_size, rows) + 1;

    OutputBlockColumns out_cols;
    std::optional<Sizes> shuffled_key_sizes;
    PaddedPODArray<ConstAggregateDataPtr> places;

    auto init_out_cols = [&]() {
        out_cols = prepareOutputBlockColumns(getHeader(), /*aggregates_pool=*/nullptr, max_block_size);
        shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);

        places.clear();
        places.reserve(max_block_size);
    };

    /// should be invoked at least once, because null data might be the only content of the `data`
    init_out_cols();

    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

    BlocksList blocks;

    size_t ttl_gc = 0;

    auto insert_columns = [&](const KeyGetter::KeyType & key) {
        auto find_result = table.findKey(key, /*disable_spill=*/true);
        if (find_result.hasError())
            throw Exception::createRuntime(find_result.errcode, find_result.errorString());

        const auto * place = static_cast<ConstAggregateDataPtr>(find_result.getMapped());
        if (place == nullptr)
        {
            if (hybrid_params->aggregate_state_ttl > 0)
            {
                /// If TTL is enabled, it is possible the key was GCed from `table`
                /// if TTL is not set correctly for `EMIT ON UPDATE WITH TIMEOUT 5m SETTING aggregate_state_ttl=3m`
                /// Ignore this key
                ++ttl_gc;
                return;
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Key was updated but was not found in source aggregation hybrid hash table");
            }
        }

        /// Regular row
        KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);
        places.push_back(place);

        /// If reached max block size, finalize the block and start a new one
        if (places.size() >= max_block_size)
        {
            blocks.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), /*arena=*/nullptr));
            table.spillIfNecessary(places.size());
            init_out_cols();
        }
    };

    auto insert_columns_udf = [&](const KeyGetter::KeyType & key) {
        auto find_result = table.findKey(key, /*disable_spill=*/true);
        if (find_result.hasError())
            throw Exception::createRuntime(find_result.errcode, find_result.errorString());

        const auto * place = static_cast<ConstAggregateDataPtr>(find_result.getMapped());
        if (place == nullptr)
        {
            if (hybrid_params->aggregate_state_ttl > 0)
            {
                /// If TTL is enabled, it is possible the key was GCed from `table`
                /// if TTL is not set correctly for `EMIT ON UPDATE WITH TIMEOUT 5m SETTING aggregate_state_ttl=3m`
                /// Ignore this key
                ++ttl_gc;
                return;
            }
            else
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Key was updated and tracked in update hybrid hash table but was not found in main aggregation hybrid hash table");
            }
        }

        /// for non-UDA or UDA without emit strategy, 'should_emit' is always true.
        /// For UDA with emit strategy, it is true only if the group should emit.
        assert(aggregate_functions.size() == 1);
        size_t emit_times = aggregate_functions[0]->getEmitTimes(place + offsets_of_aggregate_states[0]);
        if (emit_times > 0)
        {
            /// Duplicate key for each emit
            for (size_t i = 0; i < emit_times; i++)
                KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);

            /// Regular row
            places.push_back(place);
        }

        /// If reached max block size, finalize the block and start a new one
        if (places.size() >= max_block_size)
        {
            blocks.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), /*arena=*/nullptr));
            table.spillIfNecessary(places.size());
            init_out_cols();
        }
    };

    auto errcode = ErrorCodes::OK;
    if (params->group_by != IAggregatorParams::GroupBy::UserDefined)
        errcode = updates->forEachKey(insert_columns);
    else
        errcode = updates->forEachKey(insert_columns_udf);

    if (errcode != ErrorCodes::OK)
        throw Exception(errcode, "Failed to convert aggregate states to blocks, error_message'{}'", ErrorCodes::getName(errcode));

    if (ttl_gc > 0)
        LOG_WARNING(
            logger,
            "Found total_keys={} are garbage collected because of reaching TTL={} seconds",
            ttl_gc,
            hybrid_params->aggregate_state_ttl);

    if (!places.empty())
    {
        blocks.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), /*arena=*/nullptr));
        table.spillIfNecessary(places.size());
    }

    if (clear_updates)
        updates->clear();

    return blocks;
}

template <typename KeyGetter, typename Table>
BlocksList HybridAggregator::convertToBlocksForRetracts(Table & table, Table * retracts) const
{
    chassert(retracts);

    /// A special case: If there is no retract key, it means it is the first conversion (Retract is not enabled yet),
    /// convert all current state as normal
    if (retracts->empty())
    {
        auto blocks = convertToBlocksForAll<KeyGetter>(table);
        for (auto & block : blocks)
        {
            auto delta_col = ColumnInt8::create(block.rows(), static_cast<Int8>(1));
            auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);
            block.insert(ColumnWithTypeAndName{std::move(delta_col), delta_col_type, ProtonConsts::RESERVED_DELTA_FLAG});
        }
        return blocks;
    }

    auto rows = table.approximateCount();
    /// +1 for nullKeyData, if `data` doesn't have it - not a problem, just some memory for one excessive row will be preallocated
    const size_t max_block_size = std::min(params->max_block_size, rows) + 1;

    OutputBlockColumns out_cols;
    std::optional<Sizes> shuffled_key_sizes;
    PaddedPODArray<ConstAggregateDataPtr> places;

    OutputBlockColumns retract_out_cols;
    PaddedPODArray<ConstAggregateDataPtr> retract_places;

    auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);

    auto init_out_cols = [&]() {
        out_cols = prepareOutputBlockColumns(getHeader(), /*aggregates_pool=*/nullptr, max_block_size);
        shuffled_key_sizes = KeyGetter::shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
        places.clear();
        places.reserve(max_block_size);

        retract_out_cols = prepareOutputBlockColumns(getHeader(), /*aggregates_pool=*/nullptr, max_block_size);
        shuffled_key_sizes = KeyGetter::shuffleKeyColumns(retract_out_cols.raw_key_columns, key_sizes);
        retract_places.clear();
        retract_places.reserve(max_block_size);
    };

    /// should be invoked at least once, because null data might be the only content of the `data`
    init_out_cols();

    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

    BlocksList blocks;

    auto do_retract = [&](const KeyGetter::KeyType & key, auto & retract_value) {
        auto find_result = table.findKey(key, /*disable_spill=*/true);
        if (find_result.hasError())
            throw Exception::createRuntime(find_result.errcode, find_result.errorString());

        if (!find_result.isFound())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Updated key is not found in main hybrid hash table");

        auto retract = static_cast<ConstAggregateDataPtr>(retract_value.getMapped());
        if (!TrackingCount::empty(retract + tracking_count_offset)) [[likely]]
        {
            /// Retract row
            KeyGetter::insertKeyIntoColumns(key, retract_out_cols.raw_key_columns, key_sizes_ref);
            retract_places.push_back(retract);
        }

        /// return source mapped value
        return static_cast<ConstAggregateDataPtr>(find_result.getMapped());
    };

    auto do_insert_columns = [&]() {
        if (!retract_places.empty())
        {
            blocks.emplace_back(insertResultsIntoColumns(retract_places, std::move(retract_out_cols), /*arena=*/nullptr));
            auto retract_delta_col = ColumnInt8::create(retract_places.size(), static_cast<Int8>(-1));
            blocks.back().insert(ColumnWithTypeAndName{std::move(retract_delta_col), delta_col_type, ProtonConsts::RESERVED_DELTA_FLAG});
            blocks.back().setRetract();
        }

        blocks.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), /*arena=*/nullptr));
        auto delta_col = ColumnInt8::create(places.size(), static_cast<Int8>(1));
        blocks.back().insert(ColumnWithTypeAndName{std::move(delta_col), delta_col_type, ProtonConsts::RESERVED_DELTA_FLAG});

        table.spillIfNecessary(places.size());
    };

    auto insert_columns = [&](const KeyGetter::KeyType & key, auto retract_value, bool flush) {
        auto place = do_retract(key, retract_value);
        if (!TrackingCount::empty(place + tracking_count_offset)) [[likely]]
        {
            /// Regular row
            KeyGetter::insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);
            places.push_back(place);
        }
        else
        {
            /// If the key group is empty, skip the output and remove the key
            table.removeKey(key);
        }

        /// If reached max block size, finalize the block and start a new one
        if (flush || places.size() >= max_block_size)
        {
            do_insert_columns();
            init_out_cols();
        }
    };

    auto done_callback = [&]() {
        if (!places.empty())
            do_insert_columns();
    };

    auto errcode = retracts->forBatchValue(max_block_size, insert_columns, done_callback);
    if (errcode != ErrorCodes::OK)
        throw Exception(errcode, "Failed to convert aggregate states to blocks, error_message'{}'", ErrorCodes::getName(errcode));

    /// After conversion, we need clear retracts for next round of emit
    retracts->clear();

    return blocks;
}

/// Merge many data variants to one and convert the merged data variants to blocks
BlocksList HybridAggregator::mergeAndConvertToBlocks(
    ManyIAggregatedDataVariants & many_data_variants, size_t max_threads, AggregatingConvertParams & cparams) const
{
    LOG_DEBUG(logger, "Converting aggregated {} data to blocks", magic_enum::enum_name(params->tracking_updates_type));

    if (many_data_variants.size() == 1)
        return convertToBlocks(*many_data_variants.back(), max_threads, cparams);

    if (std::ranges::all_of(many_data_variants, [](const auto & data_variants) { return data_variants->empty(); }))
        return {};

    SCOPE_EXIT({
        bool clear_states = cparams.type == AggregatingConvertType::Normal && cparams.clear_state;
        if (clear_states)
        {
            for (const auto & data_variants : many_data_variants)
            {
                chassert(data_variants->aggregatorType() == AggregatorType::Hybrid);
                auto * hybrid_variants = static_cast<HybridAggregatedDataVariants *>(data_variants.get());
                hybrid_variants->reset();
            }
        }
    });

    Arena merge_arena;

    /// Merge arena is a temporary arena which is used for merge
    switch (method_chosen)
    {
        case HybridHashType::Empty:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "HybridHashTable is not inited");
        }
        case HybridHashType::key_hashed:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Hashed key shall not be chosen for aggregation");
        }
        case HybridHashType::WithoutKey:
        {
            HybridAggregatedDataVariants result{/*id_=*/"merge-result"};
            initStates(result);

            mergeWithoutKey(result, many_data_variants, merge_arena);
            return convertToBlocksWithoutKey(result);
        }

#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        using Table = decltype(HybridHashTableTemplate::NAME)::element_type; \
        std::vector<Table *> src_tables; \
        std::vector<Table *> update_tables; \
        std::vector<Table *> retract_tables; \
        src_tables.reserve(many_data_variants.size()); \
        update_tables.reserve(many_data_variants.size()); \
        retract_tables.reserve(many_data_variants.size()); \
\
        for (const auto & data_variants : many_data_variants) \
        { \
            chassert(data_variants->aggregatorType() == AggregatorType::Hybrid); \
            auto * hybrid_variants = static_cast<HybridAggregatedDataVariants *>(data_variants.get()); \
            if (!hybrid_variants->table.NAME) \
                continue; \
\
            src_tables.push_back(hybrid_variants->table.NAME.get()); \
            update_tables.push_back(hybrid_variants->updates.NAME.get()); \
            retract_tables.push_back(hybrid_variants->retracts.NAME.get()); \
        } \
\
        if (has_nullable_key) \
        { \
            using KeyGetter = HybridKeyGetter<HybridHashType::NAME, /*nullable=*/true>; \
            if (cparams.keys_already_sharded) \
            { \
                return parallelConvertToBlocks<KeyGetter>(src_tables, update_tables, retract_tables); \
            } \
            else \
            { \
                HybridAggregatedDataVariants result{/*id_=*/"merge-result"}; \
                initStates(result); \
\
                merge<KeyGetter>(*result.table.NAME, result.retracts.NAME.get(), src_tables, update_tables, retract_tables, merge_arena); \
                auto blocks = convertToBlocksMerged<KeyGetter>(*result.table.NAME, result.retracts.NAME.get()); \
\
                for (auto [update_table, retract_table] : std::views::zip(update_tables, retract_tables)) \
                { \
                    if (update_table) \
                        update_table->clear(); \
                    if (retract_table) \
                        retract_table->clear(); \
                } \
                return blocks; \
            } \
        } \
        else \
        { \
            using KeyGetter = HybridKeyGetter<HybridHashType::NAME, /*nullable=*/false>; \
            if (cparams.keys_already_sharded) \
            { \
                return parallelConvertToBlocks<KeyGetter>(src_tables, update_tables, retract_tables); \
            } \
            else \
            { \
                HybridAggregatedDataVariants result{/*id_=*/"merge-result"}; \
                initStates(result); \
\
                merge<KeyGetter>(*result.table.NAME, result.retracts.NAME.get(), src_tables, update_tables, retract_tables, merge_arena); \
                auto blocks = convertToBlocksMerged<KeyGetter>(*result.table.NAME, result.retracts.NAME.get()); \
\
                for (auto [update_table, retract_table] : std::views::zip(update_tables, retract_tables)) \
                { \
                    if (update_table) \
                        update_table->clear(); \
                    if (retract_table) \
                        retract_table->clear(); \
                } \
                return blocks; \
            } \
        } \
    }
            APPLY_FOR_HASH_KEY_VARIANTS_HYBRID(M)
#undef M
    }
}

template <typename KeyGetter, typename Table>
BlocksList HybridAggregator::parallelConvertToBlocks(
    const std::vector<Table *> & srcs, const std::vector<Table *> & src_updates, const std::vector<Table *> & src_retracts) const
{
    BlocksList blocks;

    for (size_t i = 0, num_srcs = srcs.size(); i < num_srcs; ++i)
        blocks.splice(blocks.end(), convertToBlocksImpl<KeyGetter>(*srcs[i], src_updates[i], src_retracts[i], /*clear_updates=*/true));

    return blocks;
}

void HybridAggregator::mergeWithoutKey(
    HybridAggregatedDataVariants & result, ManyIAggregatedDataVariants & many_data_variants, Arena & arena) const
{
    assert(result.without_key);

    switch (params->tracking_updates_type)
    {
        case TrackingUpdatesType::None:
            [[fallthrough]];
        case TrackingUpdatesType::Updates:
        {
            for (auto & data_variants : many_data_variants)
            {
                chassert(data_variants->aggregatorType() == AggregatorType::Hybrid);
                auto * src_variants = static_cast<HybridAggregatedDataVariants *>(data_variants.get());
                if (!src_variants->without_key)
                    continue;

                mergeAggregateStates(result.without_key.get(), src_variants->without_key.get(), &arena);
            }
            break;
        }
        case TrackingUpdatesType::UpdatesWithRetract:
        {
            auto has_retract = std::ranges::any_of(many_data_variants, [](const auto & data_variants) {
                chassert(data_variants->aggregatorType() == AggregatorType::Hybrid);
                return static_cast<HybridAggregatedDataVariants *>(data_variants.get())->without_key_retracts != nullptr;
            });

            for (auto & data_variants : many_data_variants)
            {
                auto * src_variants = static_cast<HybridAggregatedDataVariants *>(data_variants.get());
                if (!src_variants->without_key)
                    continue;

                /// 1) Merge current state
                mergeAggregateStates(result.without_key.get(), src_variants->without_key.get(), &arena);
                TrackingCount::merge(
                    result.without_key.get() + tracking_count_offset, src_variants->without_key.get() + tracking_count_offset);

                /// 2) Merge retract state
                if (!has_retract)
                    continue;

                if (!result.without_key_retracts)
                    result.initWithoutKeyRetractStates(total_size_of_aggregate_states, align_aggregate_states);

                if (src_variants->without_key_retracts)
                {
                    /// 2.1) The key was added/updated in some shard (has retract state or empty retract state), merge with the retracted state, then reset it
                    if (!TrackingCount::empty(src_variants->without_key_retracts.get()))
                    {
                        mergeAggregateStates(result.without_key_retracts.get(), src_variants->without_key_retracts.get(), &arena);
                        TrackingCount::merge(
                            result.without_key_retracts.get() + tracking_count_offset,
                            src_variants->without_key_retracts.get() + tracking_count_offset);
                    }

                    src_variants->resetRetractWithoutKey();
                }
                else
                {
                    /// 2.2) The key is not found in some shards (no retract state), merge with current state
                    mergeAggregateStates(result.without_key_retracts.get(), src_variants->without_key.get(), &arena);
                    TrackingCount::merge(
                        result.without_key_retracts.get() + tracking_count_offset, src_variants->without_key.get() + tracking_count_offset);
                }
            }
            break;
        }
    }
}

/// Merge key / values from src hash tables to dst hash table
template <typename KeyGetter, typename Table>
ALWAYS_INLINE void HybridAggregator::merge(
    Table & dst,
    Table * dst_retracts,
    const std::vector<Table *> & srcs,
    const std::vector<Table *> & src_updates,
    const std::vector<Table *> & src_retracts,
    Arena & arena) const
{
    switch (params->tracking_updates_type)
    {
        case TrackingUpdatesType::None:
        {
            mergeNormal(dst, srcs, arena);
            break;
        }
        case TrackingUpdatesType::Updates:
        {
            mergeUpdates<KeyGetter>(dst, srcs, src_updates, arena);
            break;
        }
        case TrackingUpdatesType::UpdatesWithRetract:
        {
            mergeRetracts<KeyGetter>(dst, dst_retracts, srcs, src_retracts, arena);
            break;
        }
    }
}

template <typename Table>
void HybridAggregator::mergeNormal(Table & dst, const std::vector<Table *> & srcs, Arena & arena) const
{
    for (auto * src : srcs)
    {
        assert(src != nullptr);

        /// FIXME, batch
        if (trackingStateCount())
        {
            src->forEachKeyValue([&](const Table::KeyType & key, auto value) {
                auto src_mapped = static_cast<ConstAggregateDataPtr>(value.getMapped());
                if (TrackingCount::empty(src_mapped + tracking_count_offset))
                    return;

                auto emplace_result = dst.emplaceKey(key, /*disable_spill=*/false);
                if (emplace_result.hasError())
                    throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

                auto dst_mapped = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());
                mergeAggregateStates(dst_mapped, src_mapped, &arena);
                TrackingCount::merge(dst_mapped + tracking_count_offset, src_mapped + tracking_count_offset);
            });
        }
        else
        {
            src->forEachKeyValue([&](const Table::KeyType & key, auto value) {
                auto src_mapped = static_cast<ConstAggregateDataPtr>(value.getMapped());
                auto emplace_result = dst.emplaceKey(key, /*disable_spill=*/false);
                if (emplace_result.hasError())
                    throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

                auto dst_mapped = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());
                mergeAggregateStates(dst_mapped, src_mapped, &arena);
            });
        }
    }
}

template <typename KeyGetter, typename Table>
void HybridAggregator::mergeUpdates(
    Table & dst, const std::vector<Table *> & srcs, const std::vector<Table *> & src_updates, Arena & arena) const
{
    for (size_t i = 0, num_srcs = src_updates.size(); i < num_srcs; ++i)
    {
        if (!src_updates[i])
            continue;

        /// FIXME, batch
        if (trackingStateCount())
        {
            src_updates[i]->forEachKey([&](const Table::KeyType & key) {
                auto find_result = srcs[i]->findKey(key, /*disable_spill=*/false);
                if (find_result.hasError())
                    throw Exception::createRuntime(find_result.errcode, find_result.errorString());

                if (find_result.isNotFound())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Key is not found in main hybrid hash table");

                auto emplace_result = dst.emplaceKey(key, /*disable_spill=*/false);
                if (emplace_result.hasError())
                    throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

                auto src_mapped = static_cast<ConstAggregateDataPtr>(find_result.getMapped());
                auto dst_mapped = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());
                mergeAggregateStates(dst_mapped, src_mapped, &arena);
                TrackingCount::merge(dst_mapped + tracking_count_offset, src_mapped + tracking_count_offset);
            });
        }
        else
        {
            src_updates[i]->forEachKey([&](const Table::KeyType & key) {
                auto find_result = srcs[i]->findKey(key, /*disable_spill=*/false);
                if (find_result.hasError())
                    throw Exception::createRuntime(find_result.errcode, find_result.errorString());

                if (find_result.isNotFound())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Key is not found in main hybrid hash table");

                auto emplace_result = dst.emplaceKey(key, /*disable_spill=*/false);
                if (emplace_result.hasError())
                    throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

                auto src_mapped = static_cast<ConstAggregateDataPtr>(find_result.getMapped());
                auto dst_mapped = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());
                mergeAggregateStates(dst_mapped, src_mapped, &arena);
            });
        }
    }
}


template <typename KeyGetter, typename Table>
void HybridAggregator::mergeRetracts(
    Table & dst, Table * dst_retracts, const std::vector<Table *> & srcs, const std::vector<Table *> & src_retracts, Arena & arena) const
{
    chassert(dst_retracts);
    chassert(trackingStateCount());

    /// First, collect all retracted keys (including new keys) to dst_retracts
    /// For example:
    ///                 (thread)        (thread-2)      (thread-3)
    ///     key-1       retract         non-retract     non-retract
    ///     key-2       non-retract     retract         non-retract
    ///     key-3       non-retract     non-retract     non-retract
    /// The collected keys: [key-1, key-2]
    for (auto * src_retract : src_retracts)
    {
        if (!src_retract)
            continue;

        src_retract->forEachKey([&](const KeyGetter::KeyType & key) {
            auto dst_retracts_emplace_result = dst_retracts->emplaceKey(key, /*disable_spill=*/false);
            if (dst_retracts_emplace_result.hasError())
                throw Exception::createRuntime(dst_retracts_emplace_result.errorCode(), dst_retracts_emplace_result.errorString());
        });
    }

    /// Second, merge current/retracted aggregate state of collected keys to dst/dst_retracts
    /// A special case: If there is no collected key, it means it is the first conversion (Retract is not enabled yet),
    /// merge all current state as normal
    if (dst_retracts->empty())
        return mergeNormal(dst, srcs, arena);

    dst_retracts->forEachKeyValue([&](const KeyGetter::KeyType & key, auto retract_value) {
        for (auto [src, src_retract] : std::views::zip(srcs, src_retracts))
        {
            if (!src)
                continue;

            auto find_result = src->findKey(key, /*disable_spill=*/false);
            if (find_result.hasError())
                throw Exception::createRuntime(find_result.errcode, find_result.errorString());

            if (!find_result.isFound())
                continue;

            /// 1) Merge current state to dst
            auto emplace_result = dst.emplaceKey(key, /*disable_spill=*/false);
            if (emplace_result.hasError())
                throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

            auto src_mapped = static_cast<ConstAggregateDataPtr>(find_result.getMapped());
            auto dst_mapped = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());
            mergeAggregateStates(dst_mapped, src_mapped, &arena);
            TrackingCount::merge(dst_mapped + tracking_count_offset, src_mapped + tracking_count_offset);

            /// 2) Merge retracted state to dst_retracts
            /// 2.1) The key was updated in some shard (has retract data), merge with the retracted state
            auto dst_retract = static_cast<AggregateDataPtr>(retract_value.getMutableMapped());
            if (src_retract)
            {
                auto retract_find_result = src_retract->findKey(key, /*disable_spill=*/false);
                if (retract_find_result.hasError())
                    throw Exception::createRuntime(retract_find_result.errcode, retract_find_result.errorString());

                if (retract_find_result.isFound())
                {
                    auto src_retract_mapped = static_cast<ConstAggregateDataPtr>(retract_find_result.getMapped());
                    mergeAggregateStates(dst_retract, src_retract_mapped, &arena);
                    TrackingCount::merge(dst_retract + tracking_count_offset, src_retract_mapped + tracking_count_offset);
                    return;
                }
            }

            /// 2.2) The key is not found in some shards (no retract data), merge with current state
            mergeAggregateStates(dst_retract, src_mapped, &arena);
            TrackingCount::merge(dst_retract + tracking_count_offset, src_mapped + tracking_count_offset);
        }
    });
}

}

}
