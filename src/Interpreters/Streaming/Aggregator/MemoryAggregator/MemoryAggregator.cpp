#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>

#include <AggregateFunctions/AggregateFunctionState.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/NativeWriter.h>
#include <IO/Operators.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/CompiledAggregateFunctionsHolder.h>
#include <Common/HashMapsTemplate.h>
#include <Common/VersionRevision.h>
#include <Common/logger_useful.h>


namespace CurrentMetrics
{
extern const Metric TemporaryFilesForAggregation;
}

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
extern const int TOO_MANY_ROWS;
extern const int EMPTY_DATA_PASSED;
extern const int CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS;
extern const int LOGICAL_ERROR;
extern const int AGGREGATE_FUNCTION_NOT_APPLICABLE;
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{
MemoryAggregator::MemoryAggregator(const Block & input_header_, const MemoryAggregatorParamsPtr & memory_params_)
    : IAggregator(memory_params_, input_header_, "StreamingAggregator")
    , memory_params(memory_params_)
    , tmp_data(
          memory_params->tmp_data_scope
              ? std::make_unique<TemporaryDataOnDisk>(memory_params->tmp_data_scope, CurrentMetrics::TemporaryFilesForAggregation)
              : nullptr)
{
    if (memory_params->overflow_row) [[unlikely]]
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Overflow row processing is not implemented in streaming aggregation");

    /// Use query-level memory tracker
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            memory_usage_before_aggregation = memory_tracker->get();

    aggregate_functions.resize(params->aggregates_size);
    for (size_t i = 0; i < params->aggregates_size; ++i)
        aggregate_functions[i] = params->aggregates[i].function.get();

    /// Initialize sizes of aggregation states and its offsets.
    offsets_of_aggregate_states.resize(params->aggregates_size);
    total_size_of_aggregate_states = 0;
    all_aggregates_has_trivial_destructor = true;

    switch (params->tracking_updates_type)
    {
        case TrackingUpdatesType::UpdatesWithRetract:
        {
            total_size_of_aggregate_states = sizeof(TrackingUpdatesWithRetract);
            align_aggregate_states = alignof(TrackingUpdatesWithRetract);
            break;
        }
        case TrackingUpdatesType::Updates:
        {
            total_size_of_aggregate_states = sizeof(TrackingUpdates);
            align_aggregate_states = alignof(TrackingUpdates);
            break;
        }
        case TrackingUpdatesType::None:
            break;
    }

    // aggregate_states will be aligned as below:
    // |<-- UpdatesTrackingData -->|<-- state_1 -->|<-- pad_1 -->|<-- state_2 -->|<-- pad_2 -->| .....
    //
    // pad_N will be used to match alignment requirement for each next state.
    // The address of state_1 is aligned based on maximum alignment requirements in states
    for (size_t i = 0; i < params->aggregates_size; ++i)
    {
        offsets_of_aggregate_states[i] = total_size_of_aggregate_states;

        total_size_of_aggregate_states += params->aggregates[i].function->sizeOfData();

        // aggregate states are aligned based on maximum requirement
        align_aggregate_states = std::max(align_aggregate_states, params->aggregates[i].function->alignOfData());

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < params->aggregates_size)
        {
            size_t alignment_of_next_state = params->aggregates[i + 1].function->alignOfData();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: alignOfData is not 2^N");

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            total_size_of_aggregate_states
                = (total_size_of_aggregate_states + alignment_of_next_state - 1) / alignment_of_next_state * alignment_of_next_state;
        }

        if (!params->aggregates[i].function->hasTrivialDestructor())
            all_aggregates_has_trivial_destructor = false;
    }

    method_chosen = chooseAggregationMethod();
    HashMethodContext::Settings cache_settings;
    cache_settings.max_threads = params->max_threads;
    aggregation_state_cache = MemoryAggregatedDataVariants::createCache(method_chosen, cache_settings);

#if USE_EMBEDDED_COMPILER
    compileAggregateFunctionsIfNeeded();
#endif
}

MemoryAggregatedDataVariants::Type MemoryAggregator::chooseAggregationMethod()
{
    /// If no keys. All aggregating to single row.
    if (params->keys_size == 0)
        return MemoryAggregatedDataVariants::Type::without_key;

    /// Check if at least one of the specified keys is nullable.
    DataTypes types_removed_nullable;
    types_removed_nullable.reserve(params->keys.size());
    bool has_nullable_key = false;
    bool has_low_cardinality = false;

    for (const auto & key : params->keys)
    {
        DataTypePtr type = input_header.getByName(key).type;

        if (type->lowCardinality())
        {
            has_low_cardinality = true;
            type = removeLowCardinality(type);
        }

        if (type->isNullable())
        {
            has_nullable_key = true;
            type = removeNullable(type);
        }

        types_removed_nullable.push_back(type);
    }

    /** Returns ordinary (not two-level) methods, because we start from them.
      * Later, during aggregation process, data may be converted (partitioned) to two-level structure, if cardinality is high.
      */

    size_t keys_bytes = 0;
    size_t num_fixed_contiguous_keys = 0;

    key_sizes.resize(params->keys_size);
    for (size_t j = 0; j < params->keys_size; ++j)
    {
        if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            {
                ++num_fixed_contiguous_keys;
                key_sizes[j] = types_removed_nullable[j]->getSizeOfValueInMemory();
                keys_bytes += key_sizes[j];
            }
        }
    }

    auto method_type = chooseAggregationMethodTimeBucketTwoLevel(
        types_removed_nullable, has_nullable_key, has_low_cardinality, num_fixed_contiguous_keys, keys_bytes);
    if (method_type)
        return method_type.value();

    if (has_nullable_key)
    {
        if (params->keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                return MemoryAggregatedDataVariants::Type::nullable_keys128;
            if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
                return MemoryAggregatedDataVariants::Type::nullable_keys256;
        }

        if (has_low_cardinality && params->keys_size == 1)
        {
            if (types_removed_nullable[0]->isValueRepresentedByNumber())
            {
                size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

                if (size_of_field == 1)
                    return MemoryAggregatedDataVariants::Type::low_cardinality_key8;
                if (size_of_field == 2)
                    return MemoryAggregatedDataVariants::Type::low_cardinality_key16;
                if (size_of_field == 4)
                    return MemoryAggregatedDataVariants::Type::low_cardinality_key32;
                if (size_of_field == 8)
                    return MemoryAggregatedDataVariants::Type::low_cardinality_key64;
            }
            else if (isString(types_removed_nullable[0]))
                return MemoryAggregatedDataVariants::Type::low_cardinality_key_string;
            else if (isFixedString(types_removed_nullable[0]))
                return MemoryAggregatedDataVariants::Type::low_cardinality_key_fixed_string;
        }

        /// Fallback case.
        return MemoryAggregatedDataVariants::Type::serialized;
    }

    /// No key has been found to be nullable.

    /// Single numeric key.
    if (params->keys_size == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
    {
        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (has_low_cardinality)
        {
            if (size_of_field == 1)
                return MemoryAggregatedDataVariants::Type::low_cardinality_key8;
            if (size_of_field == 2)
                return MemoryAggregatedDataVariants::Type::low_cardinality_key16;
            if (size_of_field == 4)
                return MemoryAggregatedDataVariants::Type::low_cardinality_key32;
            if (size_of_field == 8)
                return MemoryAggregatedDataVariants::Type::low_cardinality_key64;
            if (size_of_field == 16)
                return MemoryAggregatedDataVariants::Type::low_cardinality_keys128;
            if (size_of_field == 32)
                return MemoryAggregatedDataVariants::Type::low_cardinality_keys256;
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Logical error: low cardinality numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.");
        }

        if (size_of_field == 1)
            return MemoryAggregatedDataVariants::Type::key8;
        if (size_of_field == 2)
            return MemoryAggregatedDataVariants::Type::key16;
        if (size_of_field == 4)
            return MemoryAggregatedDataVariants::Type::key32;
        if (size_of_field == 8)
            return MemoryAggregatedDataVariants::Type::key64;
        if (size_of_field == 16)
            return MemoryAggregatedDataVariants::Type::keys128;
        if (size_of_field == 32)
            return MemoryAggregatedDataVariants::Type::keys256;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.");
    }

    if (params->keys_size == 1 && isFixedString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return MemoryAggregatedDataVariants::Type::low_cardinality_key_fixed_string;
        else
            return MemoryAggregatedDataVariants::Type::key_fixed_string;
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (params->keys_size == num_fixed_contiguous_keys)
    {
        if (has_low_cardinality)
        {
            if (keys_bytes <= 16)
                return MemoryAggregatedDataVariants::Type::low_cardinality_keys128;
            if (keys_bytes <= 32)
                return MemoryAggregatedDataVariants::Type::low_cardinality_keys256;
        }

        if (keys_bytes <= 2)
            return MemoryAggregatedDataVariants::Type::keys16;
        if (keys_bytes <= 4)
            return MemoryAggregatedDataVariants::Type::keys32;
        if (keys_bytes <= 8)
            return MemoryAggregatedDataVariants::Type::keys64;
        if (keys_bytes <= 16)
            return MemoryAggregatedDataVariants::Type::keys128;
        if (keys_bytes <= 32)
            return MemoryAggregatedDataVariants::Type::keys256;
    }

    /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
    if (params->keys_size == 1 && isString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return MemoryAggregatedDataVariants::Type::low_cardinality_key_string;
        else
            return MemoryAggregatedDataVariants::Type::key_string;
    }

    return MemoryAggregatedDataVariants::Type::serialized;
}

std::optional<MemoryAggregatedDataVariants::Type> MemoryAggregator::chooseAggregationMethodTimeBucketTwoLevel(
    const DataTypes & types_removed_nullable,
    bool has_nullable_key,
    bool has_low_cardinality,
    size_t num_fixed_contiguous_keys,
    size_t keys_bytes) const
{
    if (params->group_by != IAggregatorParams::GroupBy::WindowEnd && params->group_by != IAggregatorParams::GroupBy::WindowStart)
        return {};

    /// By default, window key is placed at the beginning of the group by columns
    bucket_key_offset = 0;

    /// Fixed key packing ref AggregationCommon.h
    if (has_nullable_key)
    {
        if (params->keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
            {
                bucket_key_offset = getBitmapSize<UInt128>();
                return MemoryAggregatedDataVariants::Type::time_bucket_nullable_keys128_two_level;
            }
            if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
            {
                bucket_key_offset = getBitmapSize<UInt256>();
                return MemoryAggregatedDataVariants::Type::time_bucket_nullable_keys256_two_level;
            }
        }

        /// Fallback case.
        return MemoryAggregatedDataVariants::Type::time_bucket_serialized_two_level;
    }

    /// No key has been found to be nullable.

    /// Single numeric key.
    if (params->keys_size == 1)
    {
        chassert(types_removed_nullable[0]->isValueRepresentedByNumber());

        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (size_of_field == 2)
            return MemoryAggregatedDataVariants::Type::time_bucket_key16_two_level;
        if (size_of_field == 4)
            return MemoryAggregatedDataVariants::Type::time_bucket_key32_two_level;
        if (size_of_field == 8)
            return MemoryAggregatedDataVariants::Type::time_bucket_key64_two_level;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: the first streaming aggregation column has sizeOfField not in 2, 4, 8.");
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (params->keys_size == num_fixed_contiguous_keys)
    {
        chassert(keys_bytes > 2);

        if (has_low_cardinality)
        {
            if (keys_bytes <= 16)
                return MemoryAggregatedDataVariants::Type::time_bucket_low_cardinality_keys128_two_level;

            if (keys_bytes <= 32)
                return MemoryAggregatedDataVariants::Type::time_bucket_low_cardinality_keys256_two_level;
        }

        bucket_key_offset = timeBucketPackedOffset(key_sizes, keys_bytes);

        if (keys_bytes <= 4)
            return MemoryAggregatedDataVariants::Type::time_bucket_keys32_two_level;
        if (keys_bytes <= 8)
            return MemoryAggregatedDataVariants::Type::time_bucket_keys64_two_level;
        if (keys_bytes <= 16)
            return MemoryAggregatedDataVariants::Type::time_bucket_keys128_two_level;
        if (keys_bytes <= 32)
            return MemoryAggregatedDataVariants::Type::time_bucket_keys256_two_level;
    }

    return MemoryAggregatedDataVariants::Type::time_bucket_serialized_two_level;
}

bool MemoryAggregator::checkLimits(size_t result_size) const
{
    if (memory_params->max_rows_to_group_by && result_size > memory_params->max_rows_to_group_by)
    {
        switch (memory_params->group_by_overflow_mode)
        {
            case OverflowMode::THROW:
                throw Exception(
                    ErrorCodes::TOO_MANY_ROWS,
                    "Limit for rows to GROUP BY exceeded: has {} rows, maximum: {}",
                    result_size,
                    memory_params->max_rows_to_group_by);

            case OverflowMode::BREAK:
                return false;

            case OverflowMode::ANY:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Streaming aggregation doesn't support 'OverflowMode::ANY'");
        }
    }

    /// Some aggregate functions cannot throw exceptions on allocations (e.g. from C malloc)
    /// but still tracks memory. Check it here.
    CurrentMemoryTracker::check();
    return true;
}

void MemoryAggregator::addSingleKeyToAggregateColumns(
    const MemoryAggregatedDataVariants & data_variants, MutableColumns & aggregate_columns) const
{
    const auto & data = data_variants.without_key;
    for (size_t i = 0; i < params->aggregates_size; ++i)
    {
        auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);
        column_aggregate_func.getData().push_back(data + offsets_of_aggregate_states[i]);
    }
}

void MemoryAggregator::addArenasToAggregateColumns(
    const MemoryAggregatedDataVariants & data_variants, MutableColumns & aggregate_columns) const
{
    for (size_t i = 0; i < params->aggregates_size; ++i)
    {
        auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);
        for (const auto & pool : data_variants.aggregates_pools)
            column_aggregate_func.addArena(pool);
    }
}

ManyMemoryAggregatedDataVariantsPtr
MemoryAggregator::prepareVariantsToMerge(ManyIAggregatedDataVariants & many_data_variants, bool always_merge_into_empty) const
{
    chassert(!many_data_variants.empty());

    LOG_TRACE(logger, "Merging aggregated data");

    auto non_empty_data = std::make_shared<ManyMemoryAggregatedDataVariants>();

    for (auto & data_variants : many_data_variants)
    {
        if (!data_variants->empty())
            non_empty_data->push_back(std::static_pointer_cast<MemoryAggregatedDataVariants>(data_variants));
    }

    if (non_empty_data->empty())
        return non_empty_data;

    if (non_empty_data->size() > 1 || always_merge_into_empty)
    {
        /// When do streaming merging, we shall not touch existing memory arenas and
        /// all memory arenas merge to the first empty one, so we need create a new resulting arena
        /// at position 0.
        auto result_variants = std::make_shared<MemoryAggregatedDataVariants>(/*id_=*/"result", false);
        result_variants->aggregator = this;
        initDataVariants(*result_variants);
        initStatesForWithoutKey(*result_variants);
        non_empty_data->insert(non_empty_data->begin(), result_variants);
    }

    /// If at least one of the options is two-level, then convert all the options into two-level ones, if there are not such.
    /// Note - perhaps it would be more optimal not to convert single-level versions before the merge, but merge them separately, at the end.

    bool has_two_level
        = std::any_of(non_empty_data->begin(), non_empty_data->end(), [](const auto & variant) { return variant->isTwoLevel(); });

    if (has_two_level)
    {
        for (auto & variant : *non_empty_data)
            if (!variant->isTwoLevel())
                variant->convertToTwoLevel();
    }

    MemoryAggregatedDataVariantsPtr & first = non_empty_data->at(0);

    for (size_t i = 1, size = non_empty_data->size(); i < size; ++i)
    {
        if (first->type != non_empty_data->at(i)->type)
            throw Exception(
                ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS, "Cannot merge different aggregated data variants.");

        /** Elements from the remaining sets can be moved to the first data set.
          * Therefore, it must own all the arenas of all other sets.
          */
        first->aggregates_pools.insert(
            first->aggregates_pools.end(), non_empty_data->at(i)->aggregates_pools.begin(), non_empty_data->at(i)->aggregates_pools.end());
    }

    chassert(first->aggregates_pools.size() == non_empty_data->size());

    return non_empty_data;
}

template <typename Method>
void NO_INLINE MemoryAggregator::convertBlockToTwoLevelImpl(
    Method & method, Arena * pool, ColumnRawPtrs & key_columns, const Block & source, std::vector<Block> & destinations) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    size_t rows = source.rows();
    size_t columns = source.columns();

    /// Create a 'selector' that will contain bucket index for every row. It will be used to scatter rows to buckets.
    IColumn::Selector selector(rows);

    /// For every row.
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (Method::low_cardinality_optimization)
        {
            if (state.isNullAt(i))
            {
                selector[i] = 0;
                continue;
            }
        }

        /// Calculate bucket number from row hash.
        auto hash = state.getHash(method.data, i, *pool);
        auto bucket = method.data.getBucketFromHash(hash);

        selector[i] = bucket;
    }

    size_t num_buckets = destinations.size();

    for (size_t column_idx = 0; column_idx < columns; ++column_idx)
    {
        const ColumnWithTypeAndName & src_col = source.getByPosition(column_idx);
        MutableColumns scattered_columns = src_col.column->scatter(num_buckets, selector);

        for (size_t bucket = 0, size = num_buckets; bucket < size; ++bucket)
        {
            if (!scattered_columns[bucket]->empty())
            {
                Block & dst = destinations[bucket];
                dst.info.bucket_num = static_cast<Int32>(bucket);
                dst.insert({std::move(scattered_columns[bucket]), src_col.type, src_col.name});
            }

            /** Inserted columns of type ColumnAggregateFunction will own states of aggregate functions
              *  by holding shared_ptr to source column. See ColumnAggregateFunction.h
              */
        }
    }
}

std::vector<Block> MemoryAggregator::convertBlockToTwoLevel(const Block & block) const
{
    if (!block)
        return {};

    MemoryAggregatedDataVariants data{/*id_=*/"temp"};
    data.aggregator = this;

    ColumnRawPtrs key_columns(params->keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params->keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    MemoryAggregatedDataVariants::Type type = method_chosen;
    data.keys_size = params->keys_size;
    data.key_sizes = key_sizes;

#define M(NAME) \
    else if (type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        type = MemoryAggregatedDataVariants::Type::NAME##_two_level; \
    }

    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M)
#undef M
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }

    data.init(type);

    size_t num_buckets = 0;

#define M(NAME) \
    else if (data.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        num_buckets = data.NAME->data.buckets().size(); \
    }

    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M)
#undef M
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }

    std::vector<Block> splitted_blocks(num_buckets);

#define M(NAME) \
    else if (data.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        convertBlockToTwoLevelImpl(*data.NAME, data.aggregates_pool, key_columns, block, splitted_blocks); \
    }

    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M)
#undef M
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }

    return splitted_blocks;
}

/// Loop the window column to find out the lower bound and set this lower bound to aggregates pool
/// Any new memory allocation (MemoryChunk) will attach this lower bound timestamp which means
/// the MemoryChunk contains states which is at and beyond this lower bound timestamp
void MemoryAggregator::setupAggregatesPoolTimestamps(
    size_t row_begin, size_t row_end, const ColumnRawPtrs & key_columns, Arena * aggregates_pool) const
{
    if (params->group_by != IAggregatorParams::GroupBy::WindowStart && params->group_by != IAggregatorParams::GroupBy::WindowEnd)
        return;

    Int64 max_timestamp = std::numeric_limits<Int64>::min();

    /// FIXME, can we avoid this loop ?
    auto & window_col = *key_columns[0];
    for (size_t i = row_begin; i < row_end; ++i)
    {
        auto window = window_col.getInt(i);
        if (window > max_timestamp)
            max_timestamp = window;
    }
    aggregates_pool->setCurrentTimestamp(max_timestamp);
    LOG_DEBUG(logger, "Set current pool timestamp watermark={}", max_timestamp);
}

void MemoryAggregator::removeBucketsBefore(IAggregatedDataVariants & variants_result, Int64 max_bucket, UInt64 transform_id) const
{
    chassert(variants_result.aggregatorType() == AggregatorType::Memory);
    auto & result = static_cast<MemoryAggregatedDataVariants &>(variants_result);

    if (result.empty())
        return;

    auto destroy = [&](AggregateDataPtr & data) { destroyAggregateStates(data); };

    size_t removed = 0;
    Int64 last_removed_time_bucket = 0;
    size_t remaining = 0;

    switch (result.type)
    {
#define M(NAME) \
    case MemoryAggregatedDataVariants::Type::NAME: \
        std::tie(removed, last_removed_time_bucket, remaining) = result.NAME->data.removeBucketsBefore(max_bucket, destroy); \
        break;
        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M

        default:
            break;
    }

    auto [removed_arenas, removed_arena_bytes] = result.removeBucketsBefore(max_bucket);

    Arena::Stats stats;

    if (removed)
        stats = result.aggregates_pool->free(last_removed_time_bucket);

    LOG_DEBUG(
        logger,
        "Removed {} windows less or equal to {}={} keeping window_count={} remaining_windows={}. "
        "Bucket arenas: removed_arenas={} removed_arena_bytes={} remaining_arenas={} "
        "Global arena: arena_chunks={} arena_size={} chunks_removed={} bytes_removed={}. chunks_reused={} bytes_reused={} "
        "head_chunk_size={}, "
        "free_list_hits={} free_list_missed={} chunks_in_free_lists={} size_in_bytes_in_free_lists={} transform_id={}",
        removed,
        params->group_by == IAggregatorParams::GroupBy::WindowEnd ? "window_end" : "window_start",
        max_bucket,
        params->streaming_window_count,
        remaining,
        removed_arenas,
        removed_arena_bytes,
        result.remainingBucketArenas(),
        stats.chunks,
        stats.bytes,
        stats.chunks_removed,
        stats.bytes_removed,
        stats.chunks_reused,
        stats.bytes_reused,
        stats.head_chunk_size,
        stats.free_list_hits,
        stats.free_list_misses,
        stats.chunks_in_free_lists,
        stats.size_in_bytes_in_free_lists,
        transform_id);
}

std::vector<Int64> MemoryAggregator::bucketsBefore(const MemoryAggregatedDataVariants & result, Int64 max_bucket) const
{
    switch (result.type)
    {
#define M(NAME) \
    case MemoryAggregatedDataVariants::Type::NAME: \
        return result.NAME->data.bucketsBefore(max_bucket);
        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M

        default:
            break;
    }

    return {};
}

std::vector<Int64> MemoryAggregator::buckets(const IAggregatedDataVariants & variants_result) const
{
    chassert(variants_result.aggregatorType() == AggregatorType::Memory);
    const auto & result = static_cast<const MemoryAggregatedDataVariants &>(variants_result);

    switch (result.type)
    {
#define M(NAME) \
    case MemoryAggregatedDataVariants::Type::NAME: \
        return result.NAME->data.buckets();
        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M

        default:
            break;
    }

    return {};
}

void MemoryAggregator::resetUpdatedForBuckets(IAggregatedDataVariants & variants, const std::vector<Int64> & gcd_buckets) const
{
    chassert(variants.aggregatorType() == AggregatorType::Memory);
    auto & data_variants = static_cast<MemoryAggregatedDataVariants &>(variants);

    if (data_variants.empty())
        return;

    chassert(data_variants.isTimeBucketTwoLevel());
    switch (data_variants.type)
    {
#define M(NAME) \
    case MemoryAggregatedDataVariants::Type::NAME: \
    { \
        for (auto bucket : gcd_buckets) \
        { \
            if (data_variants.NAME->data.isBucketUpdated(bucket)) \
            { \
                data_variants.NAME->data.impls[bucket].forEachMapped([&](auto & mapped) { TrackingUpdates::resetUpdated(mapped); }); \
                data_variants.NAME->data.resetUpdatedBucket(bucket); \
            } \
        } \
        return; \
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

void MemoryAggregator::initDataVariants(MemoryAggregatedDataVariants & result) const
{
    result.key_sizes = key_sizes;
    result.key_offset = bucket_key_offset;

    result.init(method_chosen);

    if (params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract)
        result.resetAndCreateRetractPool();
}

}

}
