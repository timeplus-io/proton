#include <Interpreters/Streaming/Aggregator.h>

#include <future>
#include <Poco/Util/Application.h>

#include <AggregateFunctions/AggregateFunctionArray.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <Columns/ColumnTuple.h>
#include <Compression/CompressedWriteBuffer.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/NativeWriter.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>

/// proton: starts
#include <Core/LightChunk.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <Interpreters/CompiledAggregateFunctionsHolder.h>
#include <Interpreters/Streaming/AggregatedDataMetrics.h>
#include <Interpreters/Streaming/AggregationUtils.h>
#include <Common/HashMapsTemplate.h>
#include <Common/VersionRevision.h>
#include <Common/logger_useful.h>
/// proton: ends


namespace ProfileEvents
{
    extern const Event ExternalAggregationWritePart;
    extern const Event ExternalAggregationCompressedBytes;
    extern const Event ExternalAggregationUncompressedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int NOT_ENOUGH_SPACE;
    extern const int TOO_MANY_ROWS;
    extern const int EMPTY_DATA_PASSED;
    extern const int CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS;
    extern const int LOGICAL_ERROR;
    extern const int RECOVER_CHECKPOINT_FAILED;
    extern const int AGGREGATE_FUNCTION_NOT_APPLICABLE;
    extern const int NOT_IMPLEMENTED;
    extern const int SERVER_REVISION_IS_TOO_OLD;
}

namespace Streaming
{
namespace
{
inline bool worthConvertToTwoLevel(
    size_t group_by_two_level_threshold, size_t result_size, size_t group_by_two_level_threshold_bytes, Int64 result_size_bytes)
{
    /// params.group_by_two_level_threshold will be equal to 0 if we have only one thread to execute aggregation (refer to AggregatingStep::transformPipeline).
    return (group_by_two_level_threshold && result_size >= group_by_two_level_threshold)
        || (group_by_two_level_threshold_bytes && result_size_bytes >= static_cast<Int64>(group_by_two_level_threshold_bytes));
}

inline void initDataVariants(
    AggregatedDataVariants & result, AggregatedDataVariants::Type method_chosen, const Sizes & key_sizes, const Aggregator::Params & params)
{
    result.keys_size = params.keys_size;
    result.key_sizes = key_sizes;
    result.init(method_chosen);

    if (params.tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract)
        result.resetAndCreateRetractPool();
}

Columns materializeKeyColumns(Columns & columns, ColumnRawPtrs & key_columns, const Aggregator::Params & params, bool is_low_cardinality)
{
    Columns materialized_columns;
    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
    {
        materialized_columns.push_back(columns.at(params.keys[i])->convertToFullColumnIfConst());
        key_columns[i] = materialized_columns.back().get();

        if (!is_low_cardinality)
        {
            auto column_no_lc = recursiveRemoveLowCardinality(key_columns[i]->getPtr());
            if (column_no_lc.get() != key_columns[i])
            {
                materialized_columns.emplace_back(std::move(column_no_lc));
                key_columns[i] = materialized_columns.back().get();
            }
        }
    }

    return materialized_columns;
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
        SCOPE_EXIT_SAFE(cancelled.test_and_set(););

        for (size_t thread_id = 0; thread_id < num_threads; ++thread_id)
        {
            thread_pool->scheduleOrThrowOnError([thread_id, group = CurrentThread::getGroup(), results, &converter, &cancelled] {
                CurrentThread::attachToIfDetached(group);
                SCOPE_EXIT_SAFE( CurrentThread::detachQueryIfNotDetached() );
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

AggregatedDataVariants::~AggregatedDataVariants()
{
    if (aggregator && !aggregator->all_aggregates_has_trivial_destructor)
    {
        try
        {
            aggregator->destroyAllAggregateStates(*this);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void AggregatedDataVariants::reset()
{
    assert(aggregator);
    /// Clear states
    if (!aggregator->all_aggregates_has_trivial_destructor)
        aggregator->destroyAllAggregateStates(*this);

    /// Clear hash map
    switch (type)
    {
        case AggregatedDataVariants::Type::EMPTY:       break;
        case AggregatedDataVariants::Type::without_key: break;

    #define M(NAME, IS_TWO_LEVEL) \
        case AggregatedDataVariants::Type::NAME: NAME.reset(); break;
        APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
    #undef M
    }
    invalidate();

    /// Reset pool
    resetAndCreateAggregatesPools();
    retract_pool.reset();
}

void AggregatedDataVariants::convertToTwoLevel()
{
    if (aggregator)
        LOG_INFO(aggregator->log, "Converting aggregation data to two-level.");

    switch (type)
    {
    #define M(NAME) \
        case Type::NAME: \
            NAME ## _two_level = std::make_unique<decltype(NAME ## _two_level)::element_type>(*(NAME)); \
            (NAME).reset(); \
            type = Type::NAME ## _two_level; \
            break;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M)

    #undef M

        default:
            throw Exception("Wrong data variant passed.", ErrorCodes::LOGICAL_ERROR);
    }
}

void AggregatedDataVariants::serialize(WriteBuffer & wb, const Aggregator & aggregator_) const
{
    /// We cannot use itself `aggregator` since if there is no data, it is nullptr.
    aggregator_.checkpoint(*this, wb);
}

void AggregatedDataVariants::deserialize(ReadBuffer & rb, const Aggregator & aggregator_)
{
    aggregator_.recover(*this, rb);
}

Block Aggregator::getHeader(bool final) const
{
    return params.getHeader(final);
}

Block Aggregator::Params::getHeader(
    const Block & src_header,
    const Block & intermediate_header,
    const ColumnNumbers & keys,
    const AggregateDescriptions & aggregates,
    bool final)
{
    Block res;

    if (intermediate_header)
    {
        res = intermediate_header.cloneEmpty();

        if (final)
        {
            for (const auto & aggregate : aggregates)
            {
                auto & elem = res.getByName(aggregate.column_name);

                elem.type = aggregate.function->getReturnType();
                elem.column = elem.type->createColumn();
            }
        }
    }
    else
    {
        for (const auto & key : keys)
            res.insert(src_header.safeGetByPosition(key).cloneEmpty());

        for (const auto & aggregate : aggregates)
        {
            size_t arguments_size = aggregate.arguments.size();
            DataTypes argument_types(arguments_size);
            for (size_t j = 0; j < arguments_size; ++j)
                argument_types[j] = src_header.safeGetByPosition(aggregate.arguments[j]).type;

            DataTypePtr type;
            if (final)
                type = aggregate.function->getReturnType();
            else
                type = std::make_shared<DataTypeAggregateFunction>(aggregate.function, argument_types, aggregate.parameters);

            res.insert({ type, aggregate.column_name });
        }
    }

    return materializeBlock(res);
}

void Aggregator::Params::explain(WriteBuffer & out, size_t indent) const
{
    Strings res;
    const auto & header = src_header ? src_header
                                     : intermediate_header;

    String prefix(indent, ' ');

    {
        /// Dump keys.
        out << prefix << "Keys: ";

        bool first = true;
        for (auto key : keys)
        {
            if (!first)
                out << ", ";
            first = false;

            if (key >= header.columns())
                out << "unknown position " << key;
            else
                out << header.getByPosition(key).name;
        }

        out << '\n';
    }

    if (!aggregates.empty())
    {
        out << prefix << "Aggregates:\n";

        for (const auto & aggregate : aggregates)
            aggregate.explain(out, indent + 4);
    }
}

void Aggregator::Params::explain(JSONBuilder::JSONMap & map) const
{
    const auto & header = src_header ? src_header
                                     : intermediate_header;

    auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

    for (auto key : keys)
    {
        if (key >= header.columns())
            keys_array->add("");
        else
            keys_array->add(header.getByPosition(key).name);
    }

    map.add("Keys", std::move(keys_array));

    if (!aggregates.empty())
    {
        auto aggregates_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & aggregate : aggregates)
        {
            auto aggregate_map = std::make_unique<JSONBuilder::JSONMap>();
            aggregate.explain(*aggregate_map);
            aggregates_array->add(std::move(aggregate_map));
        }

        map.add("Aggregates", std::move(aggregates_array));
    }
}

Aggregator::Aggregator(const Params & params_) : params(params_),  log(&Poco::Logger::get("StreamingAggregator"))
{
    if (params.overflow_row) [[unlikely]]
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Overflow row processing is not implemented in streaming aggregation");

    /// Use query-level memory tracker
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            memory_usage_before_aggregation = memory_tracker->get();

    aggregate_functions.resize(params.aggregates_size);
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_functions[i] = params.aggregates[i].function.get();

    /// Initialize sizes of aggregation states and its offsets.
    offsets_of_aggregate_states.resize(params.aggregates_size);
    total_size_of_aggregate_states = 0;
    all_aggregates_has_trivial_destructor = true;

    switch (trackingUpdatesType())
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
    // |<-- [UpdatesTrackingData] -->||<-- state_1 -->|<-- pad_1 -->|<-- state_2 -->|<-- pad_2 -->| .....
    //
    // pad_N will be used to match alignment requirement for each next state.
    // The address of state_1 is aligned based on maximum alignment requirements in states
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        offsets_of_aggregate_states[i] = total_size_of_aggregate_states;

        total_size_of_aggregate_states += params.aggregates[i].function->sizeOfData();

        // aggregate states are aligned based on maximum requirement
        align_aggregate_states = std::max(align_aggregate_states, params.aggregates[i].function->alignOfData());

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < params.aggregates_size)
        {
            size_t alignment_of_next_state = params.aggregates[i + 1].function->alignOfData();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0)
                throw Exception("Logical error: alignOfData is not 2^N", ErrorCodes::LOGICAL_ERROR);

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            total_size_of_aggregate_states = (total_size_of_aggregate_states + alignment_of_next_state - 1) / alignment_of_next_state * alignment_of_next_state;
        }

        if (!params.aggregates[i].function->hasTrivialDestructor())
            all_aggregates_has_trivial_destructor = false;
    }

    method_chosen = chooseAggregationMethod();
    HashMethodContext::Settings cache_settings;
    cache_settings.max_threads = params.max_threads;
    aggregation_state_cache = AggregatedDataVariants::createCache(method_chosen, cache_settings);

#if USE_EMBEDDED_COMPILER
    /// TODO: Support compile aggregate functions
    // compileAggregateFunctionsIfNeeded();
#endif
}

#if USE_EMBEDDED_COMPILER
/*
void Aggregator::compileAggregateFunctionsIfNeeded()
{
    static std::unordered_map<UInt128, UInt64, UInt128Hash> aggregate_functions_description_to_count;
    static std::mutex mtx;

    if (!params.compile_aggregate_expressions)
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
            AggregateFunctionWithOffset function_to_compile
            {
                .function = function,
                .aggregate_data_offset = offset_of_aggregate_function
            };

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
        std::lock_guard<std::mutex> lock(mtx);

        if (aggregate_functions_description_to_count[aggregate_functions_description_hash_key]++ < params.min_count_to_compile_aggregate_expression)
            return;

        if (auto * compilation_cache = CompiledExpressionCacheFactory::instance().tryGetCache())
        {
            auto [compiled_function_cache_entry, _] = compilation_cache->getOrSet(aggregate_functions_description_hash_key, [&] ()
            {
                LOG_TRACE(log, "Compile expression {}", functions_description);

                auto compiled_aggregate_functions = compileAggregateFunctions(getJITInstance(), functions_to_compile, functions_description);
                return std::make_shared<CompiledAggregateFunctionsHolder>(std::move(compiled_aggregate_functions));
            });

            compiled_aggregate_functions_holder = std::static_pointer_cast<CompiledAggregateFunctionsHolder>(compiled_function_cache_entry);
        }
        else
        {
            LOG_TRACE(log, "Compile expression {}", functions_description);
            auto compiled_aggregate_functions = compileAggregateFunctions(getJITInstance(), functions_to_compile, functions_description);
            compiled_aggregate_functions_holder = std::make_shared<CompiledAggregateFunctionsHolder>(std::move(compiled_aggregate_functions));
        }
    }
}
*/
#endif

AggregatedDataVariants::Type Aggregator::chooseAggregationMethod()
{
    /// If no keys. All aggregating to single row.
    if (params.keys_size == 0)
        return AggregatedDataVariants::Type::without_key;

    /// Check if at least one of the specified keys is nullable.
    DataTypes types_removed_nullable;
    types_removed_nullable.reserve(params.keys.size());
    bool has_nullable_key = false;
    bool has_low_cardinality = false;

    for (const auto & pos : params.keys)
    {
        DataTypePtr type = (params.src_header ? params.src_header : params.intermediate_header).safeGetByPosition(pos).type;

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

    key_sizes.resize(params.keys_size);
    for (size_t j = 0; j < params.keys_size; ++j)
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

    /// proton: starts
    auto method_type = chooseAggregationMethodTimeBucketTwoLevel(
        types_removed_nullable, has_nullable_key, has_low_cardinality, num_fixed_contiguous_keys, keys_bytes);
    if (method_type != AggregatedDataVariants::Type::EMPTY)
        return method_type;
    /// proton: ends

    if (has_nullable_key)
    {
        if (params.keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                return AggregatedDataVariants::Type::nullable_keys128;
            if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
                return AggregatedDataVariants::Type::nullable_keys256;
        }

        if (has_low_cardinality && params.keys_size == 1)
        {
            if (types_removed_nullable[0]->isValueRepresentedByNumber())
            {
                size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

                if (size_of_field == 1)
                    return AggregatedDataVariants::Type::low_cardinality_key8;
                if (size_of_field == 2)
                    return AggregatedDataVariants::Type::low_cardinality_key16;
                if (size_of_field == 4)
                    return AggregatedDataVariants::Type::low_cardinality_key32;
                if (size_of_field == 8)
                    return AggregatedDataVariants::Type::low_cardinality_key64;
            }
            else if (isString(types_removed_nullable[0]))
                return AggregatedDataVariants::Type::low_cardinality_key_string;
            else if (isFixedString(types_removed_nullable[0]))
                return AggregatedDataVariants::Type::low_cardinality_key_fixed_string;
        }

        /// Fallback case.
        return AggregatedDataVariants::Type::serialized;
    }

    /// No key has been found to be nullable.

    /// Single numeric key.
    if (params.keys_size == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
    {
        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (has_low_cardinality)
        {
            if (size_of_field == 1)
                return AggregatedDataVariants::Type::low_cardinality_key8;
            if (size_of_field == 2)
                return AggregatedDataVariants::Type::low_cardinality_key16;
            if (size_of_field == 4)
                return AggregatedDataVariants::Type::low_cardinality_key32;
            if (size_of_field == 8)
                return AggregatedDataVariants::Type::low_cardinality_key64;
            if (size_of_field == 16)
                return AggregatedDataVariants::Type::low_cardinality_keys128;
            if (size_of_field == 32)
                return AggregatedDataVariants::Type::low_cardinality_keys256;
            throw Exception("Logical error: low cardinality numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
        }

        if (size_of_field == 1)
            return AggregatedDataVariants::Type::key8;
        if (size_of_field == 2)
            return AggregatedDataVariants::Type::key16;
        if (size_of_field == 4)
            return AggregatedDataVariants::Type::key32;
        if (size_of_field == 8)
            return AggregatedDataVariants::Type::key64;
        if (size_of_field == 16)
            return AggregatedDataVariants::Type::keys128;
        if (size_of_field == 32)
            return AggregatedDataVariants::Type::keys256;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
    }

    if (params.keys_size == 1 && isFixedString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return AggregatedDataVariants::Type::low_cardinality_key_fixed_string;
        else
            return AggregatedDataVariants::Type::key_fixed_string;
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (params.keys_size == num_fixed_contiguous_keys)
    {
        if (has_low_cardinality)
        {
            if (keys_bytes <= 16)
                return AggregatedDataVariants::Type::low_cardinality_keys128;
            if (keys_bytes <= 32)
                return AggregatedDataVariants::Type::low_cardinality_keys256;
        }

        if (keys_bytes <= 2)
            return AggregatedDataVariants::Type::keys16;
        if (keys_bytes <= 4)
            return AggregatedDataVariants::Type::keys32;
        if (keys_bytes <= 8)
            return AggregatedDataVariants::Type::keys64;
        if (keys_bytes <= 16)
            return AggregatedDataVariants::Type::keys128;
        if (keys_bytes <= 32)
            return AggregatedDataVariants::Type::keys256;
    }

    /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
    if (params.keys_size == 1 && isString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return AggregatedDataVariants::Type::low_cardinality_key_string;
        else
            return AggregatedDataVariants::Type::key_string;
    }

    return AggregatedDataVariants::Type::serialized;
}

/// proton: starts
AggregatedDataVariants::Type Aggregator::chooseAggregationMethodTimeBucketTwoLevel(
    const DataTypes & types_removed_nullable, bool has_nullable_key,
    bool has_low_cardinality, size_t num_fixed_contiguous_keys, size_t keys_bytes) const
{
    if (params.group_by != Params::GroupBy::WINDOW_END
        && params.group_by != Params::GroupBy::WINDOW_START)
        return AggregatedDataVariants::Type::EMPTY;

    if (has_nullable_key)
    {
        if (params.keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                return AggregatedDataVariants::Type::time_bucket_nullable_keys128_two_level;
            if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
                return AggregatedDataVariants::Type::time_bucket_nullable_keys256_two_level;
        }

        /// Fallback case.
        return AggregatedDataVariants::Type::time_bucket_serialized_two_level;
    }

    /// No key has been found to be nullable.

    /// Single numeric key.
    if (params.keys_size == 1)
    {
        assert(types_removed_nullable[0]->isValueRepresentedByNumber());

        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (size_of_field == 2)
            return AggregatedDataVariants::Type::time_bucket_key16_two_level;
        if (size_of_field == 4)
            return AggregatedDataVariants::Type::time_bucket_key32_two_level;
        if (size_of_field == 8)
            return AggregatedDataVariants::Type::time_bucket_key64_two_level;

        throw Exception("Logical error: the first streaming aggregation column has sizeOfField not in 2, 4, 8.", ErrorCodes::LOGICAL_ERROR);
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (params.keys_size == num_fixed_contiguous_keys)
    {
        assert(keys_bytes > 2);

        if (has_low_cardinality)
        {
            if (keys_bytes <= 16)
                return AggregatedDataVariants::Type::time_bucket_low_cardinality_keys128_two_level;
            if (keys_bytes <= 32)
                return AggregatedDataVariants::Type::time_bucket_low_cardinality_keys256_two_level;
        }

        if (keys_bytes <= 4)
            return AggregatedDataVariants::Type::time_bucket_keys32_two_level;
        if (keys_bytes <= 8)
            return AggregatedDataVariants::Type::time_bucket_keys64_two_level;
        if (keys_bytes <= 16)
            return AggregatedDataVariants::Type::time_bucket_keys128_two_level;
        if (keys_bytes <= 32)
            return AggregatedDataVariants::Type::time_bucket_keys256_two_level;
    }

    return AggregatedDataVariants::Type::time_bucket_serialized_two_level;
}
/// proton: ends

void Aggregator::createAggregateStates(AggregateDataPtr & aggregate_data, bool prefix_with_updates_tracking_state) const
{
    /// Initialize reserved TrackingUpdates
    assert(aggregate_data);
    if (prefix_with_updates_tracking_state)
    {
        switch (trackingUpdatesType())
        {
            case TrackingUpdatesType::UpdatesWithRetract:
            {
                new (aggregate_data) TrackingUpdatesWithRetract();
                break;
            }
            case TrackingUpdatesType::Updates:
            {
                new (aggregate_data) TrackingUpdates();
                break;
            }
            case TrackingUpdatesType::None:
                break;
        }
    }

    for (size_t j = 0; j < params.aggregates_size; ++j)
    {
        try
        {
            /** An exception may occur if there is a shortage of memory.
              * In order that then everything is properly destroyed, we "roll back" some of the created states.
              * The code is not very convenient.
              */
            aggregate_functions[j]->create(aggregate_data + offsets_of_aggregate_states[j]);
        }
        catch (...)
        {
            for (size_t rollback_j = 0; rollback_j < j; ++rollback_j)
                aggregate_functions[rollback_j]->destroy(aggregate_data + offsets_of_aggregate_states[rollback_j]);

            throw;
        }
    }
}

[[nodiscard]] bool Aggregator::executeImpl(
    AggregatedDataVariants & result,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions) const
{
    #define M(NAME, IS_TWO_LEVEL) \
        else if (result.type == AggregatedDataVariants::Type::NAME) \
            return executeImplBatch(*result.NAME, result.aggregates_pool, row_begin, row_end, key_columns, aggregate_instructions);

    if (false) {} // NOLINT
    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
    #undef M

    return false;
}

/** It's interesting - if you remove `noinline`, then gcc for some reason will inline this function, and the performance decreases (~ 10%).
  * (Probably because after the inline of this function, more internal functions no longer be inlined.)
  * Inline does not make sense, since the inner loop is entirely inside this function.
  */
template <typename Method>
[[nodiscard]] bool NO_INLINE Aggregator::executeImplBatch(
    Method & method,
    Arena * aggregates_pool,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    /// Optimization for special case when there are no aggregate functions.
    if (params.aggregates_size == 0 && !needTrackUpdates())
    {
        /// For all rows.
        AggregateDataPtr place = aggregates_pool->alloc(0);
        for (size_t i = row_begin; i < row_end; ++i)
            state.emplaceKey(method.data, i, *aggregates_pool).setMapped(place);
        return false;
    }

    bool need_finalization = false;

    /// Optimization for special case when aggregating by 8bit key.
    if constexpr (std::is_same_v<Method, typename decltype(AggregatedDataVariants::key8)::element_type>)
    {
        /// We use another method if there are aggregate functions with -Array combinator.
        bool has_arrays = false;
        for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
        {
            if (inst->offsets)
            {
                has_arrays = true;
                break;
            }
        }

        if (!has_arrays && !needTrackUpdates())
        {
            for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
            {
                inst->batch_that->addBatchLookupTable8(
                    row_begin,
                    row_end,
                    reinterpret_cast<AggregateDataPtr *>(method.data.data()),
                    inst->state_offset,
                    [&](AggregateDataPtr & aggregate_data)
                    {
                        auto data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                        createAggregateStates(data, /*prefix_with_updates_tracking_state=*/ false);
                        aggregate_data = data;
                    },
                    state.getKeyData(),
                    inst->batch_arguments,
                    aggregates_pool,
                    inst->delta_column);

                /// Calculate if we need finalization
                if (!need_finalization && inst->batch_that->isUserDefined())
                {
                    auto * map = reinterpret_cast<AggregateDataPtr *>(method.data.data());
                    const auto * key = state.getKeyData();
                    for (size_t cur = row_begin; !need_finalization && cur < row_end; ++cur)
                        need_finalization = (inst->batch_that->getEmitTimes(map[key[cur]] + inst->state_offset) > 0);
                }
            }
            return need_finalization;
        }
    }

    /// NOTE: only row_end-row_start is required, but:
    /// - this affects only optimize_aggregation_in_order,
    /// - this is just a pointer, so it should not be significant,
    /// - and plus this will require other changes in the interface.
    std::unique_ptr<AggregateDataPtr[]> places(new AggregateDataPtr[row_end]);

    /// For all rows.
    for (size_t i = row_begin; i < row_end; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplaceKey(method.data, i, *aggregates_pool);

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.isInserted())
        {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.setMapped(nullptr);

            aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data);

            emplace_result.setMapped(aggregate_data);
        }
        else
            aggregate_data = emplace_result.getMapped();

        assert(aggregate_data != nullptr);
        places[i] = aggregate_data;
    }

    /// Add values to the aggregate functions.
    for (size_t i = 0; i < aggregate_functions.size(); ++i)
    {
        AggregateFunctionInstruction * inst = aggregate_instructions + i;

        if (inst->offsets)
            inst->batch_that->addBatchArray(row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, inst->offsets, aggregates_pool);
        else
            inst->batch_that->addBatch(row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, aggregates_pool, -1, inst->delta_column);

        if (inst->batch_that->isUserDefined())
        {
            AggregateDataPtr * places_ptr = places.get();
            /// It is ok to re-flush if it is flush already, then we don't need maintain a map to check if it is ready flushed
            for (size_t j = row_begin; j < row_end; ++j)
            {
                if (places_ptr[j])
                {
                    inst->batch_that->flush(places_ptr[j] + inst->state_offset);
                    if (!need_finalization)
                        need_finalization = (inst->batch_that->getEmitTimes(places_ptr[j] + inst->state_offset) > 0);
                }
            }
        }
    }

    if (needTrackUpdates())
        TrackingUpdates::addBatch(row_begin, row_end, places.get(), aggregate_instructions ? aggregate_instructions->delta_column : nullptr);

    return need_finalization;
}

[[nodiscard]] bool NO_INLINE Aggregator::executeWithoutKeyImpl(
    AggregatedDataWithoutKey & res,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    Arena * arena) const
{
    /// Adding values
    bool should_finalize = false;
    for (size_t i = 0; i < aggregate_functions.size(); ++i)
    {
        AggregateFunctionInstruction * inst = aggregate_instructions + i;

        if (inst->offsets)
            inst->batch_that->addBatchSinglePlace(
                inst->offsets[static_cast<ssize_t>(row_begin) - 1],
                inst->offsets[row_end - 1],
                res + inst->state_offset,
                inst->batch_arguments,
                arena,
                -1,
                inst->delta_column);
        else
            inst->batch_that->addBatchSinglePlace(
                row_begin,
                row_end,
                res + inst->state_offset,
                inst->batch_arguments,
                arena,
                -1,
                inst->delta_column);

        if (inst->batch_that->isUserDefined())
        {
            inst->batch_that->flush(res + inst->state_offset);

            if (!should_finalize)
                should_finalize = (inst->batch_that->getEmitTimes(res + inst->state_offset) > 0);
        }
    }

    if (needTrackUpdates())
        TrackingUpdates::addBatchSinglePlace(row_begin, row_end, res, aggregate_instructions ? aggregate_instructions->delta_column : nullptr);

    return should_finalize;
}

void Aggregator::prepareAggregateInstructions(Columns columns, AggregateColumns & aggregate_columns, Columns & materialized_columns,
     AggregateFunctionInstructions & aggregate_functions_instructions, NestedColumnsHolder & nested_columns_holder) const
{
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_columns[i].resize(params.aggregates[i].arguments.size());

    aggregate_functions_instructions.resize(params.aggregates_size + 1);
    aggregate_functions_instructions[params.aggregates_size].that = nullptr;

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
        {
            materialized_columns.push_back(columns.at(params.aggregates[i].arguments[j])->convertToFullColumnIfConst());
            aggregate_columns[i][j] = materialized_columns.back().get();

            auto column_no_lc = recursiveRemoveLowCardinality(aggregate_columns[i][j]->getPtr());
            if (column_no_lc.get() != aggregate_columns[i][j])
            {
                materialized_columns.emplace_back(std::move(column_no_lc));
                aggregate_columns[i][j] = materialized_columns.back().get();
            }
        }

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
            aggregate_functions_instructions[i].batch_arguments = aggregate_columns[i].data();

        aggregate_functions_instructions[i].batch_that = that;

        /// proton : starts
        if (params.delta_col_pos >= 0)
            aggregate_functions_instructions[i].delta_column = columns.at(params.delta_col_pos).get(); /// point to the column point is ok
        /// proton : ends
    }
}

/// return {should_abort, need_finalization}
std::pair<bool, bool> Aggregator::executeOnBlock(
    const Block & block,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns) const
{
    return executeOnBlock(
        block.getColumns(),
        /* row_begin= */ 0,
        block.rows(),
        result,
        key_columns,
        aggregate_columns);
}

/// return {should_abort, need_finalization}
std::pair<bool, bool> Aggregator::executeOnBlock(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns) const
{
    std::pair<bool, bool> return_result = {false, false};
    auto & need_abort = return_result.first;
    auto & need_finalization = return_result.second;

    /// `result will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    /// How to perform the aggregation?
    if (result.empty())
    {
        /// proton: starts. First init the key_sizes, and then the method since method init
        /// may depend on the key_sizes
        initDataVariants(result, method_chosen, key_sizes, params);
        initStatesForWithoutKey(result);
        /// proton: ends
        LOG_TRACE(log, "Aggregation method: {}", result.getMethodName());
    }

    /** Constant columns are not supported directly during aggregation.
      * To make them work anyway, we materialize them.
      */
    Columns materialized_columns = materializeKeyColumns(columns, key_columns, params, result.isLowCardinality());

    /// proton: starts. For window start/end aggregation, we will need setup timestamp of the aggr to
    /// enable memory recycling.
    setupAggregatesPoolTimestamps(row_begin, row_end, key_columns, result.aggregates_pool);
    /// proton: ends

    NestedColumnsHolder nested_columns_holder;
    AggregateFunctionInstructions aggregate_functions_instructions;
    prepareAggregateInstructions(columns, aggregate_columns, materialized_columns, aggregate_functions_instructions, nested_columns_holder);

    /// We select one of the aggregation methods and call it.

    /// For the case when there are no keys (all aggregate into one row).
    if (result.type == AggregatedDataVariants::Type::without_key)
        need_finalization = executeWithoutKeyImpl(result.without_key, row_begin, row_end, aggregate_functions_instructions.data(), result.aggregates_pool);
    else
        need_finalization = executeImpl(result, row_begin, row_end, key_columns, aggregate_functions_instructions.data());

    need_abort = checkAndProcessResult(result);
    return return_result;
}

void Aggregator::writeToTemporaryFile(AggregatedDataVariants & data_variants, const String & tmp_path) const
{
    Stopwatch watch;
    size_t rows = data_variants.size();

    auto file = createTemporaryFile(tmp_path);
    const std::string & path = file->path();
    WriteBufferFromFile file_buf(path);
    CompressedWriteBuffer compressed_buf(file_buf);
    NativeWriter block_out(compressed_buf, getHeader(false), ProtonRevision::getVersionRevision());

    LOG_DEBUG(log, "Writing part of aggregation data into temporary file {}.", path);
    ProfileEvents::increment(ProfileEvents::ExternalAggregationWritePart);

    /// Flush only two-level data and possibly overflow data.

#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        writeToTemporaryFileImpl(data_variants, *data_variants.NAME, block_out);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_ALL_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    /// NOTE Instead of freeing up memory and creating new hash tables and arenas, you can re-use the old ones.
    data_variants.init(data_variants.type);
    data_variants.aggregates_pools = Arenas(1, std::make_shared<Arena>());
    data_variants.aggregates_pool = data_variants.aggregates_pools.back().get();
    initStatesForWithoutKey(data_variants);

    block_out.flush();
    compressed_buf.next();
    file_buf.next();

    double elapsed_seconds = watch.elapsedSeconds();
    size_t compressed_bytes = file_buf.count();
    size_t uncompressed_bytes = compressed_buf.count();

    {
        std::lock_guard lock(temporary_files.mutex);
        temporary_files.files.emplace_back(std::move(file));
        temporary_files.sum_size_uncompressed += uncompressed_bytes;
        temporary_files.sum_size_compressed += compressed_bytes;
    }

    ProfileEvents::increment(ProfileEvents::ExternalAggregationCompressedBytes, compressed_bytes);
    ProfileEvents::increment(ProfileEvents::ExternalAggregationUncompressedBytes, uncompressed_bytes);

    LOG_DEBUG(log,
        "Written part in {:.3f} sec., {} rows, {} uncompressed, {} compressed,"
        " {:.3f} uncompressed bytes per row, {:.3f} compressed bytes per row, compression rate: {:.3f}"
        " ({:.3f} rows/sec., {}/sec. uncompressed, {}/sec. compressed)",
        elapsed_seconds,
        rows,
        ReadableSize(uncompressed_bytes),
        ReadableSize(compressed_bytes),
        static_cast<double>(uncompressed_bytes) / rows,
        static_cast<double>(compressed_bytes) / rows,
        static_cast<double>(uncompressed_bytes) / compressed_bytes,
        rows / elapsed_seconds,
        ReadableSize(static_cast<double>(uncompressed_bytes) / elapsed_seconds),
        ReadableSize(static_cast<double>(compressed_bytes) / elapsed_seconds));
}


void Aggregator::writeToTemporaryFile(AggregatedDataVariants & data_variants) const
{
    String tmp_path = params.tmp_volume->getDisk()->getPath();
    return writeToTemporaryFile(data_variants, tmp_path);
}


template <typename Method>
Block Aggregator::convertOneBucketToBlockImpl(
    AggregatedDataVariants & data_variants,
    Method & method,
    Arena * arena,
    bool final,
    bool clear_states,
    Int64 bucket,
    ConvertType type) const
{
    if (type == ConvertType::Updates && !method.data.isBucketUpdated(bucket))
        return {};

    constexpr bool return_single_block = true;
    Block block = convertToBlockImpl<return_single_block>(method, method.data.impls[bucket], arena, data_variants.aggregates_pools, final, method.data.impls[bucket].size(), clear_states, type);
    block.info.bucket_num = static_cast<int>(bucket);
    method.data.resetUpdatedBucket(bucket); /// finalized
    return block;
}

template <typename Method>
void Aggregator::writeToTemporaryFileImpl(
    AggregatedDataVariants & data_variants,
    Method & method,
    NativeWriter & out) const
{
    size_t max_temporary_block_size_rows = 0;
    size_t max_temporary_block_size_bytes = 0;

    auto update_max_sizes = [&](const Block & block)
    {
        size_t block_size_rows = block.rows();
        size_t block_size_bytes = block.bytes();

        if (block_size_rows > max_temporary_block_size_rows)
            max_temporary_block_size_rows = block_size_rows;
        if (block_size_bytes > max_temporary_block_size_bytes)
            max_temporary_block_size_bytes = block_size_bytes;
    };

    for (auto bucket : method.data.buckets())
    {
        Block block = convertOneBucketToBlockImpl(data_variants, method, data_variants.aggregates_pool, false, false, bucket);
        out.write(block);
        update_max_sizes(block);
    }

    /// Pass ownership of the aggregate functions states:
    /// `data_variants` will not destroy them in the destructor, they are now owned by ColumnAggregateFunction objects.
    data_variants.aggregator = nullptr;

    LOG_DEBUG(log, "Max size of temporary block: {} rows, {}.", max_temporary_block_size_rows, ReadableSize(max_temporary_block_size_bytes));
}


bool Aggregator::checkLimits(size_t result_size) const
{
    if (params.max_rows_to_group_by && result_size > params.max_rows_to_group_by)
    {
        switch (params.group_by_overflow_mode)
        {
            case OverflowMode::THROW:
                throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(result_size)
                    + " rows, maximum: " + toString(params.max_rows_to_group_by),
                    ErrorCodes::TOO_MANY_ROWS);

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


template <bool return_single_block, typename Method, typename Table>
Aggregator::ConvertToBlockRes<return_single_block>
Aggregator::convertToBlockImpl(
    Method & method, Table & data, Arena * arena, Arenas & aggregates_pools, bool final, size_t rows, bool clear_states, ConvertType type) const
{
    if (data.empty())
    {
        auto && out_cols = prepareOutputBlockColumns(params, aggregate_functions, getHeader(final), aggregates_pools, final, rows);
        return {finalizeBlock(params, getHeader(final), std::move(out_cols), final, rows)};
    }

    ConvertToBlockRes<return_single_block> res;

    if (final)
    {
        res = convertToBlockImplFinal<return_single_block>(method, data, arena, aggregates_pools, rows, clear_states, type);
    }
    else
    {
        assert(type == ConvertType::Normal);
        res = convertToBlockImplNotFinal<return_single_block>(method, data, aggregates_pools, rows);
    }

    return res;
}


template <typename Mapped>
inline void Aggregator::insertAggregatesIntoColumns(
    Mapped & mapped,
    MutableColumns & final_aggregate_columns,
    Arena * arena,
    bool clear_states) const
{
    /** Final values of aggregate functions are inserted to columns.
      * Then states of aggregate functions, that are not longer needed, are destroyed.
      *
      * We mark already destroyed states with "nullptr" in data,
      *  so they will not be destroyed in destructor of Aggregator
      * (other values will be destroyed in destructor in case of exception).
      *
      * But it becomes tricky, because we have multiple aggregate states pointed by a single pointer in data.
      * So, if exception is thrown in the middle of moving states for different aggregate functions,
      *  we have to catch exceptions and destroy all the states that are no longer needed,
      *  to keep the data in consistent state.
      *
      * It is also tricky, because there are aggregate functions with "-State" modifier.
      * When we call "insertResultInto" for them, they insert a pointer to the state to ColumnAggregateFunction
      *  and ColumnAggregateFunction will take ownership of this state.
      * So, for aggregate functions with "-State" modifier, the state must not be destroyed
      *  after it has been transferred to ColumnAggregateFunction.
      * But we should mark that the data no longer owns these states.
      */

    size_t insert_i = 0;
    std::exception_ptr exception;

    try
    {
        /// Insert final values of aggregate functions into columns.
        for (; insert_i < params.aggregates_size; ++insert_i)
            aggregate_functions[insert_i]->insertResultInto(
                mapped + offsets_of_aggregate_states[insert_i], *final_aggregate_columns[insert_i], arena);
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    /// proton: starts
    /// For streaming aggregation, we hold up to the states
    if (!clear_states)
    {
        if (exception)
            std::rethrow_exception(exception);

        return;
    }
    /// proton: ends

    /** Destroy states that are no longer needed. This loop does not throw.
        *
        * Don't destroy states for "-State" aggregate functions,
        *  because the ownership of this state is transferred to ColumnAggregateFunction
        *  and ColumnAggregateFunction will take care.
        *
        * But it's only for states that has been transferred to ColumnAggregateFunction
        *  before exception has been thrown;
        */
    for (size_t destroy_i = 0; destroy_i < params.aggregates_size; ++destroy_i)
    {
        /// If ownership was not transferred to ColumnAggregateFunction.
        if (!(destroy_i < insert_i && aggregate_functions[destroy_i]->isState()))
            aggregate_functions[destroy_i]->destroy(
                mapped + offsets_of_aggregate_states[destroy_i]);
    }

    /// Mark the cell as destroyed so it will not be destroyed in destructor.
    mapped = nullptr;

    if (exception)
        std::rethrow_exception(exception);
}

Block Aggregator::insertResultsIntoColumns(PaddedPODArray<AggregateDataPtr> & places, OutputBlockColumns && out_cols, Arena * arena, bool clear_states) const
{
    std::exception_ptr exception;
    size_t aggregate_functions_destroy_index = 0;

    try
    {
        for (; aggregate_functions_destroy_index < params.aggregates_size;)
        {
            auto & final_aggregate_column = out_cols.final_aggregate_columns[aggregate_functions_destroy_index];
            size_t offset = offsets_of_aggregate_states[aggregate_functions_destroy_index];

            /** We increase aggregate_functions_destroy_index because by function contract if insertResultIntoBatch
              * throws exception, it also must destroy all necessary states.
              * Then code need to continue to destroy other aggregate function states with next function index.
              */
            size_t destroy_index = aggregate_functions_destroy_index;
            ++aggregate_functions_destroy_index;

            /// For State AggregateFunction ownership of aggregate place is passed to result column after insert
            /// proton: starts.
            /// For non-streaming cases, after calling `insertResultIntoBatch`, aggregator / data should not maintain the places
            /// anymore (to avoid double free). We have 2 cases here:
            /// 1. For non `-State` aggregations, the ownership of places doesn't get transferred to result column after insert.
            ///    So we need a way to clean the places up: either the `insertResultIntoBatch` function destroys them or
            ///    the dtor of `data` destroys them. Here we ask `insertResultIntoBatch` to destroy them by setting .
            ///    `destroy_place_after_insert` to true. The behaviors of `insertResultIntoBatch` are:
            ///    If `DB::IAggregateFunctionHelper::insertResultIntoBatch` doesn't throw, it destroys to free dtor the places since
            ///    we set `destroy_place_after_insert` to true to tell it to destroy the places. Otherwise if it throws,
            ///    it also destroys the places no matter `destroy_place_after_insert` is set or not.
            ///    This is correct behavior for non-streaming non-State aggregations.
            /// 2. For `-State` aggregations, the ownership of places gets transferred to result column after insert. So aggregator / data doesn't
            ///    need to maintain them anymore. If `insertResultIntoBatch` throws, it cleans up the places which are not inserted yet
            ///
            /// For streaming cases (FIXME window recycling / projection):
            /// 1. For non `-State` aggregations, there are different cases
            ///    a. aggregator / data still need maintain the places if it is doing non-window based incremental aggregation
            ///       and periodical projection
            ///       For example: `SELECT avg(n) FROM STREAM(table) GROUP BY xxx`.
            ///    b. aggregator / data don't need maintain the places if it is doing window-based aggregation and when windows get sealed
            /// 2. For `-State` aggregations, since the ownership of the places are transferred to the result column, aggregator / data
            ///    don't need maintain them anymore after insert.
            bool is_state = aggregate_functions[destroy_index]->isState();
            bool destroy_place_after_insert = !is_state && clear_states;
            /// proton: ends

            aggregate_functions[destroy_index]->insertResultIntoBatch(0, places.size(), places.data(), offset, *final_aggregate_column, arena, destroy_place_after_insert);
        }
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    for (; aggregate_functions_destroy_index < params.aggregates_size; ++aggregate_functions_destroy_index)
    {
        bool is_state = aggregate_functions[aggregate_functions_destroy_index]->isState();
        bool destroy_place_after_insert = !is_state && clear_states;
        if (destroy_place_after_insert)
        {
            /// When aggregate_functions[aggregate_functions_destroy_index-1] throws Exception, there are two cases:
            /// Case 1: destroy_place_after_insert == true
            /// In this case, if a exception throws, delete the state of aggregate function with exception and aggregate function executed
            /// before this function in 'insertResultIntoBatch' and delete the states of the rest aggregate functions in 'convertToBlockImplFinal'.
            /// Because in this case, place' has been set to 'nullptr' in 'convertToBlockImplFinal' already, the 'destroyImpl' method will not
            /// destroy the states twice.
            ///
            /// Case 2: destroy_place_after_insert == false
            /// in this case, do not delete the state of any aggregate function neither in 'insertResultIntoBatch' nor in 'convertToBlockImplFinal'
            /// even if a exception throws.
            /// Because in this case (place != nullptr) the 'destroyImpl' method will destroy all the states of all aggregate functions together
            /// and it will not be destroyed the states twice neither.

            /// To avoid delete the state twice, only when destroy_place_after_insert == true, it should destroy
            /// the states of the rest aggregate functions (not executed after the aggregate function with exception below.
            size_t offset = offsets_of_aggregate_states[aggregate_functions_destroy_index];
            aggregate_functions[aggregate_functions_destroy_index]->destroyBatch(0, places.size(), places.data(), offset);
        }
    }

    if (exception)
        std::rethrow_exception(exception);

    return finalizeBlock(params, getHeader(/*final=*/ true), std::move(out_cols), /*final=*/ true, places.size());
}

template <bool return_single_block, typename Method, typename Table>
Aggregator::ConvertToBlockRes<return_single_block> NO_INLINE
Aggregator::convertToBlockImplFinal(
    Method & method, Table & data, Arena * arena, Arenas & aggregates_pools, size_t, bool clear_states, ConvertType type) const
{
    /// +1 for nullKeyData, if `data` doesn't have it - not a problem, just some memory for one excessive row will be preallocated
    const size_t max_block_size = (return_single_block ? data.size() : std::min(params.max_block_size, data.size())) + 1;
    constexpr bool final = true;
    ConvertToBlockRes<return_single_block> res;

    OutputBlockColumns out_cols;
    std::optional<Sizes> shuffled_key_sizes;
    PaddedPODArray<AggregateDataPtr> places;

    auto init_out_cols = [&]()
    {
        out_cols = prepareOutputBlockColumns(params, aggregate_functions, getHeader(final), aggregates_pools, final, max_block_size);

        if constexpr (Method::low_cardinality_optimization)
        {
            if (data.hasNullKeyData())
            {
                assert(type == ConvertType::Normal);
                out_cols.key_columns[0]->insertDefault();
                insertAggregatesIntoColumns(data.getNullKeyData(), out_cols.final_aggregate_columns, arena, clear_states);
                data.hasNullKeyData() = false;
            }
        }

        shuffled_key_sizes = method.shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);

        places.clear();
        places.reserve(max_block_size);
    };

    // should be invoked at least once, because null data might be the only content of the `data`
    init_out_cols();

    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;

    bool only_updates = (type == ConvertType::Updates);
    bool only_retract = (type == ConvertType::Retract);

    data.forEachValue([&](const auto & key, auto & mapped)
    {
        if constexpr (!return_single_block)
        {
            /// If reached max block size, finalize the block and start a new one
            if (out_cols.key_columns[0]->size() >= max_block_size)
            {
                res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), arena, clear_states));
                init_out_cols();
            }
        }

        if (only_updates)
        {
            if (!TrackingUpdates::updated(mapped))
                return;

            /// Finalized it for current coverting
            TrackingUpdates::resetUpdated(mapped);
        }
        else if (only_retract)
        {
            if (!TrackingUpdatesWithRetract::hasRetract(mapped))
                return;
        }

        auto & place = only_retract ? TrackingUpdatesWithRetract::getRetract(mapped) : mapped;

        /// For UDA with own emit strategy, there are two special cases to be handled:
        /// 1. not all groups need to  be emitted. therefore proton needs to pick groups
        /// that should emits, and only emit those groups while keep other groups unchanged.
        /// 2. a single block trigger multiple emits. In this case, proton need insert the
        /// same key multiple times for each emit result of this group.

        /// for non-UDA or UDA without emit strategy, 'should_emit' is always true.
        /// For UDA with emit strategy, it is true only if the group should emit.
        size_t emit_times = 1;
        if (params.group_by == Params::GroupBy::USER_DEFINED)
        {
            assert(aggregate_functions.size() == 1);
            emit_times = aggregate_functions[0]->getEmitTimes(place + offsets_of_aggregate_states[0]);
        }

        if (emit_times > 0)
        {
            /// duplicate key for each emit
            for (size_t i = 0; i < emit_times; i++)
                method.insertKeyIntoColumns(key, out_cols.raw_key_columns, key_sizes_ref);

            places.emplace_back(place);

            /// Mark the cell as destroyed so it will not be destroyed in destructor.
            /// proton: starts. Here we push the `place` to `places`, for streaming
            /// case, we don't want aggregate function to destroy the places
            if (clear_states)
                place = nullptr;
        }
    });

    if constexpr (return_single_block)
    {
        return insertResultsIntoColumns(places, std::move(out_cols), arena, clear_states);
    }
    else
    {
        res.emplace_back(insertResultsIntoColumns(places, std::move(out_cols), arena, clear_states));
        return res;
    }
}

template <bool return_single_block, typename Method, typename Table>
Aggregator::ConvertToBlockRes<return_single_block> NO_INLINE
Aggregator::convertToBlockImplNotFinal(Method & method, Table & data, Arenas & aggregates_pools, size_t) const
{
    /// +1 for nullKeyData, if `data` doesn't have it - not a problem, just some memory for one excessive row will be preallocated
    const size_t max_block_size = (return_single_block ? data.size() : std::min(params.max_block_size, data.size())) + 1;
    constexpr bool final = false;
    ConvertToBlockRes<return_single_block> res;

    std::optional<OutputBlockColumns> out_cols;
    std::optional<Sizes> shuffled_key_sizes;
    size_t rows_in_current_block = 0;

    auto init_out_cols = [&]()
    {
        out_cols = prepareOutputBlockColumns(params, aggregate_functions, getHeader(final), aggregates_pools, final, max_block_size);

        if constexpr (Method::low_cardinality_optimization)
        {
            if (data.hasNullKeyData())
            {
                out_cols->raw_key_columns[0]->insertDefault();

                for (size_t i = 0; i < params.aggregates_size; ++i)
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

    data.forEachValue(
        [&](const auto & key, auto & mapped)
        {
            if (!out_cols.has_value())
                init_out_cols();

            const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;
            method.insertKeyIntoColumns(key, out_cols->raw_key_columns, key_sizes_ref);

            /// reserved, so push_back does not throw exceptions
            for (size_t i = 0; i < params.aggregates_size; ++i)
                out_cols->aggregate_columns_data[i]->push_back(mapped + offsets_of_aggregate_states[i]);

            /// proton: starts. For streaming aggr, we hold on to the states
            /// Since it is not final, we shall never clear the state
            /// mapped = nullptr;
            /// proton: ends.

            ++rows_in_current_block;

            if constexpr (!return_single_block)
            {
                if (rows_in_current_block >= max_block_size)
                {
                    res.emplace_back(finalizeBlock(params, getHeader(final), std::move(out_cols.value()), final, rows_in_current_block));
                    out_cols.reset();
                    rows_in_current_block = 0;
                }
            }
        });

    if constexpr (return_single_block)
    {
        return finalizeBlock(params, getHeader(final), std::move(out_cols).value(), final, rows_in_current_block);
    }
    else
    {
        if (rows_in_current_block)
            res.emplace_back(finalizeBlock(params, getHeader(final), std::move(out_cols).value(), final, rows_in_current_block));
        return res;
    }
    return res;
}

void Aggregator::addSingleKeyToAggregateColumns(
    const AggregatedDataVariants & data_variants,
    MutableColumns & aggregate_columns) const
{
    const auto & data = data_variants.without_key;
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);
        column_aggregate_func.getData().push_back(data + offsets_of_aggregate_states[i]);
    }
}

void Aggregator::addArenasToAggregateColumns(
    const AggregatedDataVariants & data_variants,
    MutableColumns & aggregate_columns) const
{
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);
        for (const auto & pool : data_variants.aggregates_pools)
            column_aggregate_func.addArena(pool);
    }
}

Block Aggregator::prepareBlockAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool clear_states, ConvertType type) const
{
    auto res_header = getHeader(final);
    size_t rows = 1;
    auto && out_cols = prepareOutputBlockColumns(params, aggregate_functions, res_header, data_variants.aggregates_pools, final, rows);
    auto && [key_columns, raw_key_columns, aggregate_columns, final_aggregate_columns, aggregate_columns_data] = out_cols;

    assert(data_variants.type == AggregatedDataVariants::Type::without_key);

    if ((type == ConvertType::Updates && !TrackingUpdates::updated(data_variants.without_key))
        || (type == ConvertType::Retract && !TrackingUpdatesWithRetract::hasRetract(data_variants.without_key)))
        return res_header.cloneEmpty();

    AggregatedDataWithoutKey & data = [&]() -> AggregateDataPtr & {
        switch (type)
        {
            case ConvertType::Updates:
            {
                TrackingUpdates::resetUpdated(data_variants.without_key);
                return data_variants.without_key;
            }
            case ConvertType::Retract:
                return TrackingUpdatesWithRetract::getRetract(data_variants.without_key);
            case ConvertType::Normal:
                return data_variants.without_key;
        }
    }();

    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong data variant passed.");

    if (!final)
    {
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_columns_data[i]->push_back(data + offsets_of_aggregate_states[i]);
        data = nullptr;
    }
    else
    {
        /// Always single-thread. It's safe to pass current arena from 'aggregates_pool'.
        insertAggregatesIntoColumns(data, final_aggregate_columns, data_variants.aggregates_pool, clear_states);
    }

    return finalizeBlock(params, res_header, std::move(out_cols), final, rows);
}

BlocksList Aggregator::prepareBlockAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final, bool clear_states, ConvertType type) const
{
    const size_t rows = data_variants.sizeWithoutOverflowRow();
    constexpr bool return_single_block = false;
#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        return convertToBlockImpl<return_single_block>(*data_variants.NAME, data_variants.NAME->data, data_variants.aggregates_pool, data_variants.aggregates_pools, final, rows, clear_states, type);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
    else throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
}

BlocksList Aggregator::prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, bool clear_states, size_t max_threads, ConvertType type) const
{
    /// TODO Make a custom threshold.
    /// TODO Use the shared thread pool with the `merge` function.
    std::unique_ptr<ThreadPool> thread_pool;
    if (max_threads > 1 && data_variants.sizeWithoutOverflowRow() > 100000
        && final && type == ConvertType::Normal) /// use single thread for non-final or retracted data or updated data
        thread_pool = std::make_unique<ThreadPool>(max_threads);

    if (false) {} // NOLINT
#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        return prepareBlocksAndFillTwoLevelImpl(data_variants, *data_variants.NAME, final, clear_states, thread_pool.get(), type);

    APPLY_FOR_VARIANTS_ALL_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}

template <typename Method>
BlocksList Aggregator::prepareBlocksAndFillTwoLevelImpl(
    AggregatedDataVariants & data_variants,
    Method & method,
    bool final,
    bool clear_states,
    ThreadPool * thread_pool,
    ConvertType type) const
{
    return convertBucketsInParallel(thread_pool, method.data.buckets(), [&](Int64 bucket, Arena * arena) -> BlocksList {
        /// Skip no changed bucket if only updated is requested
        if (type == ConvertType::Updates && !method.data.isBucketUpdated(bucket))
            return {};

        return {convertOneBucketToBlockImpl(data_variants, method, arena, final, clear_states, bucket, type)};
    });
}

BlocksList Aggregator::convertToBlocks(AggregatedDataVariants & data_variants, bool final, size_t max_threads) const
{
    LOG_DEBUG(log, "Converting aggregated data to blocks");

    Stopwatch watch;

    BlocksList blocks;

    /// In what data structure is the data aggregated?
    if (data_variants.empty())
        return blocks;

    bool clear_states = final && !params.keep_state;

    if (data_variants.type == AggregatedDataVariants::Type::without_key)
        blocks.emplace_back(prepareBlockAndFillWithoutKey(data_variants, final, clear_states, ConvertType::Normal));
    else if (!data_variants.isTwoLevel())
        blocks = prepareBlockAndFillSingleLevel(data_variants, final, clear_states, ConvertType::Normal);
    else
        blocks = prepareBlocksAndFillTwoLevel(data_variants, final, clear_states, max_threads, ConvertType::Normal);

    /// proton: starts.
    if (clear_states)
        data_variants.reset();
    /// proton: ends

    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & block : blocks)
    {
        rows += block.rows();
        bytes += block.bytes();
    }

    double elapsed_seconds = watch.elapsedSeconds();
    LOG_DEBUG(log,
        "Converted aggregated data to blocks. {} rows, {} in {} sec. ({:.3f} rows/sec., {}/sec.)",
        rows, ReadableSize(bytes),
        elapsed_seconds, rows / elapsed_seconds,
        ReadableSize(bytes / elapsed_seconds));

    return blocks;
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataNullKey(
    Table & table_dst,
    Table & table_src,
    Arena * arena,
    bool clear_states) const
{
    if constexpr (Method::low_cardinality_optimization)
    {
        if (table_src.hasNullKeyData())
        {
            if (!table_dst.hasNullKeyData())
            {
                table_dst.hasNullKeyData() = true;
                table_dst.getNullKeyData() = table_src.getNullKeyData();
            }
            else
            {
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->merge(
                            table_dst.getNullKeyData() + offsets_of_aggregate_states[i],
                            table_src.getNullKeyData() + offsets_of_aggregate_states[i],
                            arena);

                /// proton : starts
                if (clear_states)
                    for (size_t i = 0; i < params.aggregates_size; ++i)
                        aggregate_functions[i]->destroy(
                                table_src.getNullKeyData() + offsets_of_aggregate_states[i]);
            }

            if (clear_states)
            {
                table_src.hasNullKeyData() = false;
                table_src.getNullKeyData() = nullptr;
            }
            /// proton : ends
        }
    }
}


template <typename Method, typename Table, typename KeyHandler>
void NO_INLINE Aggregator::mergeDataImpl(
    Table & table_dst,
    Table & table_src,
    Arena * arena,
    bool clear_states,
    KeyHandler && key_handler) const
{
    if constexpr (Method::low_cardinality_optimization)
        mergeDataNullKey<Method, Table>(table_dst, table_src, arena, clear_states);

    auto func = [&](AggregateDataPtr & __restrict dst, AggregateDataPtr & __restrict src, bool inserted)
    {
        if (inserted)
        {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            dst = nullptr;

            /// If there are multiple sources, there are more than one AggregatedDataVariant. Aggregator always creates a new AggregatedDataVariant and merge all other
            /// AggregatedDataVariants to the new created one. After finalize(), it does not clean up aggregate state except the new create AggregatedDataVariant.
            /// If it does not alloc new memory for the 'dst' (i.e. aggregate state of the new AggregatedDataVariant which get destroyed after finalize()) but reuse
            /// that from the 'src' to store the final aggregated result, it will cause the data from other AggregatedDataVariant will be merged multiple times and
            /// generate incorrect aggregated result.
            auto aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data);
            dst = aggregate_data;
        }

        mergeAggregateStates(dst, src, arena, clear_states);
    };

    if constexpr (std::is_same_v<KeyHandler, EmptyKeyHandler>)
        table_src.template mergeToViaEmplace<decltype(func), false>(table_dst, std::move(func));
    else
    {
        table_src.forEachValue([&](const auto & key, auto & mapped)
        {
            typename Table::LookupResult res_it;
            bool inserted;
            table_dst.emplace(key_handler(key), res_it, inserted);
            func(res_it->getMapped(), mapped, inserted);
        });
    }

    /// In order to release memory early.
    if (clear_states)
        table_src.clearAndShrink();
    /// proton: ends
}

void NO_INLINE Aggregator::mergeWithoutKeyDataImpl(ManyAggregatedDataVariants & non_empty_data, bool clear_states) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        AggregatedDataVariants & current = *non_empty_data[result_num];
        mergeAggregateStates(res->without_key, current.without_key, res->aggregates_pool, clear_states);

        /// In order to release memory early.
        if (clear_states)
            current.reset();
    }
}

template <typename Method>
void NO_INLINE Aggregator::mergeSingleLevelDataImpl(ManyAggregatedDataVariants & non_empty_data, bool clear_states) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        if (!checkLimits(res->sizeWithoutOverflowRow()))
            break;

        AggregatedDataVariants & current = *non_empty_data[result_num];

        mergeDataImpl<Method>(
            getDataVariant<Method>(*res).data,
            getDataVariant<Method>(current).data,
            res->aggregates_pool,
            clear_states);

        /// In order to release memory early.
        if (clear_states)
            current.reset();
    }
}

#define M(NAME) \
    template void NO_INLINE Aggregator::mergeSingleLevelDataImpl<decltype(AggregatedDataVariants::NAME)::element_type>( \
        ManyAggregatedDataVariants & non_empty_data, bool clear_states) const;
    APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M


BlocksList
Aggregator::mergeAndConvertToBlocks(ManyAggregatedDataVariants & data_variants, bool final, size_t max_threads) const
{
    auto prepared_data_ptr = prepareVariantsToMerge(data_variants);
    if (prepared_data_ptr->empty())
        return {};

    bool clear_states = final && !params.keep_state;
    BlocksList blocks;
    auto & first = *prepared_data_ptr->at(0);
    if (first.type == AggregatedDataVariants::Type::without_key)
    {
        mergeWithoutKeyDataImpl(*prepared_data_ptr, clear_states);
        blocks.emplace_back(prepareBlockAndFillWithoutKey(first, final, clear_states, ConvertType::Normal));
    }
    else if (!first.isTwoLevel())
    {
        if (false) { } // NOLINT
#define M(NAME) \
        else if (first.type == AggregatedDataVariants::Type::NAME) \
            mergeSingleLevelDataImpl<decltype(first.NAME)::element_type>(*prepared_data_ptr, clear_states);

        APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
        else throw Exception("Unknown single level aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

        blocks = prepareBlockAndFillSingleLevel(first, final, clear_states, ConvertType::Normal);
    }
    else
    {
        auto total_size = std::accumulate(prepared_data_ptr->begin(), prepared_data_ptr->end(), 0ull, [](size_t size, const auto & variants) {
            return size + variants->sizeWithoutOverflowRow();
        });
        /// TODO Make a custom threshold.
        /// TODO Use the shared thread pool with the `merge` function.
        std::unique_ptr<ThreadPool> thread_pool;
        if (max_threads > 1 && total_size > 100000 && final)
            thread_pool = std::make_unique<ThreadPool>(max_threads);

        if (false) { } // NOLINT
#define M(NAME) \
        else if (first.type == AggregatedDataVariants::Type::NAME) \
            blocks = mergeAndConvertTwoLevelToBlocksImpl<decltype(first.NAME)::element_type>(*prepared_data_ptr, final, clear_states, thread_pool.get());

        APPLY_FOR_VARIANTS_ALL_TWO_LEVEL(M)
#undef M
        else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    if (clear_states)
    {
        for (auto & variants : *prepared_data_ptr)
            variants->reset();
    }

    return blocks;
}

template <typename Method>
BlocksList Aggregator::mergeAndConvertTwoLevelToBlocksImpl(
    ManyAggregatedDataVariants & non_empty_data, bool final, bool clear_states, ThreadPool * thread_pool) const
{
    auto & first = *non_empty_data.at(0);

    std::vector<Int64> buckets;
    if (first.isStaticBucketTwoLevel())
    {
        buckets = getDataVariant<Method>(first).data.buckets();
    }
    else
    {
        assert(first.isTimeBucketTwoLevel());
        std::unordered_set<Int64> buckets_set;
        for (auto & data_variants : non_empty_data)
        {
            auto tmp_buckets = getDataVariant<Method>(*data_variants).data.buckets();
            buckets_set.insert(tmp_buckets.begin(), tmp_buckets.end());
        }
        buckets.assign(buckets_set.begin(), buckets_set.end());
    }

    return convertBucketsInParallel(thread_pool, buckets, [&](Int64 bucket, Arena * arena) -> BlocksList {
        mergeBucketImpl<Method>(non_empty_data, bucket, arena, clear_states);
        return {convertOneBucketToBlockImpl(first, getDataVariant<Method>(first), arena, final, clear_states, bucket)};
    });
}

template <typename Method>
void NO_INLINE Aggregator::mergeBucketImpl(
    ManyAggregatedDataVariants & data, Int64 bucket, Arena * arena, bool clear_states, std::atomic<bool> * is_cancelled) const
{
    /// We merge all aggregation results to the first.
    AggregatedDataVariantsPtr & res = data[0];
    for (size_t result_num = 1, size = data.size(); result_num < size; ++result_num)
    {
        if (is_cancelled && is_cancelled->load(std::memory_order_seq_cst))
            return;

        AggregatedDataVariants & current = *data[result_num];
        mergeDataImpl<Method>(
            getDataVariant<Method>(*res).data.impls[bucket],
            getDataVariant<Method>(current).data.impls[bucket],
            arena,
            clear_states);

        /// Assume the current bucket has been finalized.
        getDataVariant<Method>(current).data.resetUpdatedBucket(bucket);
    }
}

ManyAggregatedDataVariantsPtr Aggregator::prepareVariantsToMerge(ManyAggregatedDataVariants & data_variants, bool always_merge_into_empty) const
{
    if (data_variants.empty())
        throw Exception("Empty data passed to Aggregator::prepareVariantsToMerge.", ErrorCodes::EMPTY_DATA_PASSED);

    LOG_TRACE(log, "Merging aggregated data");

    auto non_empty_data = std::make_shared<ManyAggregatedDataVariants>();

    /// proton: starts:
    for (auto & data : data_variants)
        if (!data->empty())
            non_empty_data->push_back(data);

    if (non_empty_data->empty())
        return non_empty_data;

    if (non_empty_data->size() > 1 || always_merge_into_empty)
    {
        /// When do streaming merging, we shall not touch existing memory arenas and
        /// all memory arenas merge to the first empty one, so we need create a new resulting arena
        /// at position 0.
        auto result_variants = std::make_shared<AggregatedDataVariants>(false);
        result_variants->aggregator = this;
        initDataVariants(*result_variants, method_chosen, key_sizes, params);
        initStatesForWithoutKey(*result_variants);
        non_empty_data->insert(non_empty_data->begin(), result_variants);
    }

    /// for streaming query, we don't need sort the arenas
//    if (non_empty_data.size() > 1)
//    {
//        /// Sort the states in descending order so that the merge is more efficient (since all states are merged into the first).
//        std::sort(non_empty_data.begin(), non_empty_data.end(),
//            [](const AggregatedDataVariantsPtr & lhs, const AggregatedDataVariantsPtr & rhs)
//            {
//                return lhs->sizeWithoutOverflowRow() > rhs->sizeWithoutOverflowRow();
//            });
//    }
    /// proton: ends

    /// If at least one of the options is two-level, then convert all the options into two-level ones, if there are not such.
    /// Note - perhaps it would be more optimal not to convert single-level versions before the merge, but merge them separately, at the end.

    /// proton: starts. The first variant is for result aggregating
    bool has_two_level
        = std::any_of(non_empty_data->begin(), non_empty_data->end(), [](const auto & variant) { return variant->isTwoLevel(); });

    if (has_two_level)
    {
        for (auto & variant : *non_empty_data)
            if (!variant->isTwoLevel())
                variant->convertToTwoLevel();
    }

    AggregatedDataVariantsPtr & first = non_empty_data->at(0);

    for (size_t i = 1, size = non_empty_data->size(); i < size; ++i)
    {
        if (first->type != non_empty_data->at(i)->type)
            throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

        /** Elements from the remaining sets can be moved to the first data set.
          * Therefore, it must own all the arenas of all other sets.
          */
        first->aggregates_pools.insert(first->aggregates_pools.end(),
            non_empty_data->at(i)->aggregates_pools.begin(), non_empty_data->at(i)->aggregates_pools.end());
    }

    assert(first->aggregates_pools.size() == non_empty_data->size());

    return non_empty_data;
}

template <typename Method>
void NO_INLINE Aggregator::convertBlockToTwoLevelImpl(
    Method & method,
    Arena * pool,
    ColumnRawPtrs & key_columns,
    const Block & source,
    std::vector<Block> & destinations) const
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


std::vector<Block> Aggregator::convertBlockToTwoLevel(const Block & block) const
{
    if (!block)
        return {};

    AggregatedDataVariants data;
    data.aggregator = this;

    ColumnRawPtrs key_columns(params.keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    AggregatedDataVariants::Type type = method_chosen;
    data.keys_size = params.keys_size;
    data.key_sizes = key_sizes;

#define M(NAME) \
    else if (type == AggregatedDataVariants::Type::NAME) \
        type = AggregatedDataVariants::Type::NAME ## _two_level;

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    data.init(type);

    size_t num_buckets = 0;

#define M(NAME) \
    else if (data.type == AggregatedDataVariants::Type::NAME) \
        num_buckets = data.NAME->data.buckets().size();

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    std::vector<Block> splitted_blocks(num_buckets);

#define M(NAME) \
    else if (data.type == AggregatedDataVariants::Type::NAME) \
        convertBlockToTwoLevelImpl(*data.NAME, data.aggregates_pool, \
            key_columns, block, splitted_blocks);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_STATIC_BUCKET_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    return splitted_blocks;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::destroyImpl(Table & table) const
{
    table.forEachMapped([&](AggregateDataPtr & data)
    {
        /** If an exception (usually a lack of memory, the MemoryTracker throws) arose
          *  after inserting the key into a hash table, but before creating all states of aggregate functions,
          *  then data will be equal nullptr.
          */
        destroyAggregateStates(data);
    });
}


void Aggregator::destroyWithoutKey(AggregatedDataVariants & result) const
{
    destroyAggregateStates(result.without_key);
}


void Aggregator::destroyAllAggregateStates(AggregatedDataVariants & result) const
{
    if (result.empty())
        return;

    LOG_TRACE(log, "Destroying aggregate states");

    /// In what data structure is the data aggregated?
    if (result.type == AggregatedDataVariants::Type::without_key)
        destroyWithoutKey(result);

#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        destroyImpl<decltype(result.NAME)::element_type>(result.NAME->data);

    if (false) {} // NOLINT
    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else if (result.type != AggregatedDataVariants::Type::without_key)
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}

/// proton: starts. for streaming processing
void Aggregator::initStatesForWithoutKey(AggregatedDataVariants & data_variants) const
{
    if (!data_variants.without_key && data_variants.type == AggregatedDataVariants::Type::without_key)
    {
        AggregateDataPtr place = data_variants.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        data_variants.without_key = place;
    }
}

/// Loop the window column to find out the lower bound and set this lower bound to aggregates pool
/// Any new memory allocation (MemoryChunk) will attach this lower bound timestamp which means
/// the MemoryChunk contains states which is at and beyond this lower bound timestamp
void Aggregator::setupAggregatesPoolTimestamps(size_t row_begin, size_t row_end, const ColumnRawPtrs & key_columns, Arena * aggregates_pool) const
{
    if (params.group_by != Params::GroupBy::WINDOW_START && params.group_by != Params::GroupBy::WINDOW_END)
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
    LOG_DEBUG(log, "Set current pool timestamp watermark={}", max_timestamp);
}

void Aggregator::removeBucketsBefore(AggregatedDataVariants & result, Int64 max_bucket) const
{
    if (result.empty())
        return;

    auto destroy = [&](AggregateDataPtr & data)
    {
        destroyAggregateStates(data);
    };

    size_t removed = 0;
    Int64 last_removed_time_bukect = 0;
    size_t remaining = 0;

    switch (result.type)
    {
#define M(NAME) \
            case AggregatedDataVariants::Type::NAME: \
                std::tie(removed, last_removed_time_bukect, remaining) = result.NAME->data.removeBucketsBefore(max_bucket, destroy); break;
        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M

        default:
            break;
    }

    Arena::Stats stats;

    if (removed)
        stats = result.aggregates_pool->free(last_removed_time_bukect);

    LOG_INFO(
        log,
        "Removed {} windows less or equal to {}={}, keeping window_count={}, remaining_windows={}. "
        "Arena: arena_chunks={}, arena_size={}, chunks_removed={}, bytes_removed={}. chunks_reused={}, bytes_reused={}, head_chunk_size={}, "
        "free_list_hits={}, free_list_missed={}",
        removed,
        params.group_by == Params::GroupBy::WINDOW_END ? "window_end" : "window_start",
        max_bucket,
        params.streaming_window_count,
        remaining,
        stats.chunks,
        stats.bytes,
        stats.chunks_removed,
        stats.bytes_removed,
        stats.chunks_reused,
        stats.bytes_reused,
        stats.head_chunk_size,
        stats.free_list_hits,
        stats.free_list_misses);
}

std::vector<Int64> Aggregator::bucketsBefore(const AggregatedDataVariants & result, Int64 max_bucket) const
{
    switch (result.type)
    {
#define M(NAME) \
            case AggregatedDataVariants::Type::NAME: return result.NAME->data.bucketsBefore(max_bucket);
        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M

        default:
            break;
    }

    return {};
}

std::vector<Int64> Aggregator::buckets(const AggregatedDataVariants & result) const
{
    switch (result.type)
    {
#define M(NAME) \
            case AggregatedDataVariants::Type::NAME: return result.NAME->data.buckets();
        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M

        default:
            break;
    }

    return {};
}

/// The complexity of checkpoint the state of Aggregator is a combination of the following 2 cases
/// 1) The keys can reside in hashmap or in arena
/// 2) The state can reside in arena or in the aggregation function
/// And there is a special one which is group without key
void Aggregator::checkpoint(const AggregatedDataVariants & data_variants, WriteBuffer & wb) const
{
    auto version = getVersion();
    /// Serialization layout
    /// [version] + [states layout]
    writeIntBinary(version, wb);

    if (version <= 1)
        return const_cast<Aggregator *>(this)->doCheckpointLegacy(data_variants, wb);
    else if (version <= 2)
        return doCheckpointV2(data_variants, wb);
    else
        return doCheckpointV3(data_variants, wb);
}

void Aggregator::recover(AggregatedDataVariants & data_variants, ReadBuffer & rb) const
{
    /// Serialization layout
    /// [version] + [states layout]
    VersionType recovered_version = 0;
    readIntBinary(recovered_version, rb);

    assert(recovered_version <= getVersion());
    /// So far, no broken changes from `recovered_version` to `version`.

    /// FIXME: Legacy layout needs to be cleaned after no use
    if (recovered_version <= 1)
        return const_cast<Aggregator *>(this)->doRecoverLegacy(data_variants, rb);
    else if (recovered_version <= 2)
        return doRecoverV2(data_variants, rb);
    else
        return doRecoverV3(data_variants, rb);
}

void Aggregator::doCheckpointLegacy(const AggregatedDataVariants & data_variants, WriteBuffer & wb)
{
    /// States layout: [uint8][uint16][aggr-func-state-block]
    /// FIXME: Legacy layout needs to be cleaned after no use
    UInt8 inited = 1;
    if (data_variants.empty())
    {
        /// No aggregated data yet
        inited = 0;
        writeIntBinary(inited, wb);
        return;
    }

    writeIntBinary(inited, wb);

    UInt16 num_aggr_funcs = aggregate_functions.size();
    writeIntBinary(num_aggr_funcs, wb);

    /// FIXME, set a good max_threads
    /// For ConvertAction::Checkpoint, don't clear state `data_variants`
    auto blocks = convertToBlocks(const_cast<AggregatedDataVariants &>(data_variants), false, 8);

    /// assert(!blocks.empty());

    UInt32 num_blocks = static_cast<UInt32>(blocks.size());
    writeIntBinary(num_blocks, wb);
    NativeLightChunkWriter writer(wb, getHeader(false), ProtonRevision::getVersionRevision());
    for (const auto & block : blocks)
        writer.write(block);
}

void Aggregator::doRecoverLegacy(AggregatedDataVariants & data_variants, ReadBuffer & rb)
{
    UInt8 inited = 0;
    readIntBinary(inited, rb);

    if (!inited)
        return;

    UInt16 num_aggr_funcs = 0;
    readIntBinary(num_aggr_funcs, rb);

    if (num_aggr_funcs != aggregate_functions.size())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover aggregation function checkpoint. Number of aggregation functions are not the same, checkpointed={}, "
            "current={}",
            num_aggr_funcs,
            aggregate_functions.size());

    UInt32 num_blocks = 0;
    readIntBinary(num_blocks, rb);
    BlocksList blocks;
    const auto & header = getHeader(false);
    NativeLightChunkReader reader(rb, header, ProtonRevision::getVersionRevision());
    for (size_t i = 0; i < num_blocks; ++i)
    {
        auto light_chunk = reader.read();
        blocks.emplace_back(header.cloneWithColumns(light_chunk.detachColumns()));
    }

    /// Restore data variants from blocks
    recoverStates(data_variants, blocks);
}

void Aggregator::recoverStates(AggregatedDataVariants & data_variants, BlocksList & blocks)
{
    data_variants.aggregator = this;
    if (data_variants.empty())
        initDataVariants(data_variants, method_chosen, key_sizes, params);

    if (blocks.empty())
        return;

    if (data_variants.type == AggregatedDataVariants::Type::without_key)
    {
        recoverStatesWithoutKey(data_variants, blocks);
    }
    else
    {
        if (data_variants.isTwoLevel())
        {
            /// data variants is inited with two level hashmap =>
            /// windowed aggregation cases
            recoverStatesTwoLevel(data_variants, blocks);
        }
        else
        {
            /// There are 2 cases
            /// 1) data variants is inited with single level hashmap, however the checkpoint states are 2 levels
            ///    which means data variants was converted to two level
            /// 2) data variants is inited with single level and stayed as single level
            if (blocks.front().info.hasBucketNum())
            {
                /// We will need convert data_variants to two level
                data_variants.convertToTwoLevel();
                recoverStatesTwoLevel(data_variants, blocks);
            }
            else
                recoverStatesSingleLevel(data_variants, blocks);
        }
    }
}

void Aggregator::recoverStatesWithoutKey(AggregatedDataVariants & data_variants, BlocksList & blocks)
{
    /// Only one block
    assert(blocks.size() == 1);
    auto & block = blocks.front();
    /// Only one row
    assert(block.rows() == 1);
    /// Column size is same as number of aggregated functions
    assert(block.columns() == params.aggregates_size);

    /// SELECT max(i), min(i), avg(i) FROM t;
    /// States in Block is columnar
    /// s11 | s12 | s13
    /// Convert columnar to row based with padding.
    /// s11 s12 s13
    /// We also need move the aggregation functions over since they
    /// may have internal states as well

    assert(!data_variants.without_key);
    initStatesForWithoutKey(data_variants);
    AggregatedDataWithoutKey & data = data_variants.without_key;

    AggregateColumnsData aggregate_columns(params.aggregates_size);

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        auto & aggr_func_col = typeid_cast<ColumnAggregateFunction &>(block.getByName(aggregate_column_name).column->assumeMutableRef());

        /// Move over the function
        const_cast<AggregateDescription &>(params.aggregates[i]).function = aggr_func_col.getAggregateFunction();
        aggregate_functions[i] = params.aggregates[i].function.get();

        /// Remember the columns we will work with
        aggregate_columns[i] = &aggr_func_col.getData();
    }

    /// Move over the state
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_functions[i]->move(data + offsets_of_aggregate_states[i], (*aggregate_columns[i])[0], data_variants.aggregates_pool);
}

void Aggregator::recoverStatesSingleLevel(AggregatedDataVariants & data_variants, BlocksList & blocks)
{
    /// SELECT max(i), min(i), avg(i) FROM t GROUP BY k1, k2;
    /// States in Block is columnar
    /// k11 | k12 | s11 | s12 | s13
    /// k21 | k22 | s21 | s22 | s23
    /// k31 | k32 | s31 | s32 | s33
    /// Convert columnar to row based. Basically we need re-insert keys to hashtable
    /// k11 k12 s11 s12 s13
    /// -------------------
    /// k21 k22 s21 s22 s23
    /// -------------------
    /// k31 k32 s31 s32 s33
    /// We also need move the aggregation functions over since they
    /// may have internal states as well
    /// Key columns + AggregateFunction columns

    {
        /// Move over the aggr function
        auto & block = blocks.front();

        for (size_t i = 0; i < params.aggregates_size; ++i)
        {
            const auto & aggregate_column_name = params.aggregates[i].column_name;
            auto & aggr_func_col = typeid_cast<ColumnAggregateFunction &>(block.getByName(aggregate_column_name).column->assumeMutableRef());

            /// Move over the function
            const_cast<AggregateDescription &>(params.aggregates[i]).function = aggr_func_col.getAggregateFunction();
            aggregate_functions[i] = params.aggregates[i].function.get();
        }
    }

    for (auto & block : blocks)
    {
       #define M(NAME, IS_TWO_LEVEL) \
            else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
                doRecoverStates(*data_variants.NAME, data_variants.aggregates_pool, block);

        if (false) {} // NOLINT
        APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
        #undef M
    }
}

template <typename Method>
void NO_INLINE Aggregator::doRecoverStates(Method & method, Arena * aggregates_pool, Block & block)
{
    /// Remember the key columns
    ColumnRawPtrs key_columns;
    key_columns.resize(params.keys_size);

    for (size_t k = 0; k < params.keys_size; ++k)
    {
        /// const auto & key_column_name = params.src_header.safeGetByPosition(params.keys[k]).name;
        /// key_columns[k] = block.getByName(key_column_name).column.get();

        /// Since Aggregator::Params::getHeader create header in the order of its keys
        /// we can assume the first keys_size column of block are keys
        key_columns[k] = block.getByPosition(k).column.get();
    }

    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    /// Reinsert the keys
    if (params.aggregates_size == 0)
    {
        /// SELECT k1, k2 FROM t GROUP BY k1, k2
        AggregateDataPtr place = aggregates_pool->alloc(0);
        for (size_t row = 0, rows = block.rows(); row < rows; ++row)
            state.emplaceKey(method.data, row, *aggregates_pool).setMapped(place);
        return;
    }

    /// Remember aggregate columns
    AggregateColumnsData aggregate_columns(params.aggregates_size);

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        auto & aggr_func_col = typeid_cast<ColumnAggregateFunction &>(block.getByName(aggregate_column_name).column->assumeMutableRef());
        /// Remember the columns we will work with
        aggregate_columns[i] = &aggr_func_col.getData();
    }

    for (size_t row = 0, rows = block.rows(); row < rows; ++row)
    {
        auto emplace_result = state.emplaceKey(method.data, row, *aggregates_pool);
        /// Since we are recovering from states. Each row has unique keys
        assert (emplace_result.isInserted());
        emplace_result.setMapped(nullptr);

        /// Allocate states for all aggregate functions
        AggregateDataPtr aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(aggregate_data);

        /// Move over the state
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->move(aggregate_data + offsets_of_aggregate_states[i], (*aggregate_columns[i])[row], aggregates_pool);

        emplace_result.setMapped(aggregate_data);
    }

    setupAggregatesPoolTimestamps(0, block.rows(), key_columns, aggregates_pool);
}


void Aggregator::recoverStatesTwoLevel(AggregatedDataVariants & data_variants, BlocksList & blocks)
{
    /// There are 2 cases
    /// 1. Global aggregation has 2 level
    /// 2. Windowed aggregation has 2 level (actually it may have 3 levels which can be supported in future)
    /// FIXME, concurrent recover
    recoverStatesSingleLevel(data_variants, blocks);
}

/// The complexity of checkpoint the state of Aggregator is a combination of the following 2 cases
/// 1) without key states (without_key or overflow rows)
/// 2) hash table states
void Aggregator::doCheckpointV2(const AggregatedDataVariants & data_variants, WriteBuffer & wb) const
{
    /// Serialization layout, there are 2 cases:
    /// 1) Without key: [uint8][uint16][aggr-func-state-without-key]
    /// 2) Otherwise: [uint8][uint16][aggr-func-state-for-overflow-row][is_two_level][aggr-func-state-in-hash-map]
    UInt8 inited = 1;
    if (data_variants.empty())
    {
        /// No aggregated data yet
        inited = 0;
        writeIntBinary(inited, wb);
        return;
    }

    writeIntBinary(inited, wb);

    UInt16 num_aggr_funcs = aggregate_functions.size();
    writeIntBinary(num_aggr_funcs, wb);

    /// [aggr-func-state-without-key] : 1) without key 2) overflow row
    serializeAggregateStates(data_variants.without_key, wb);
    if (data_variants.type == AggregatedDataVariants::Type::without_key)
        return;

    UInt8 is_two_level = data_variants.isTwoLevel() ? 1 : 0;
    writeIntBinary(is_two_level, wb);

    if (false)
    {
    } // NOLINT
#define M(NAME, IS_TWO_LEVEL) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) DB::serializeHashMap( \
        data_variants.NAME->data, [this](const auto & mapped, WriteBuffer & wb_) { serializeAggregateStates(mapped, wb_); }, wb);
    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}

void Aggregator::doRecoverV2(AggregatedDataVariants & data_variants, ReadBuffer & rb) const
{
    UInt8 inited = 0;
    readIntBinary(inited, rb);

    if (!inited)
        return;

    UInt16 num_aggr_funcs = 0;
    readIntBinary(num_aggr_funcs, rb);

    if (num_aggr_funcs != aggregate_functions.size())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover aggregation function checkpoint. Number of aggregation functions are not the same, checkpointed={}, "
            "current={}",
            num_aggr_funcs,
            aggregate_functions.size());

    data_variants.aggregator = this;
    if (data_variants.empty())
        initDataVariants(data_variants, method_chosen, key_sizes, params);

    /// [aggr-func-state-without-key] : 1) without key 2) overflow row
    deserializeAggregateStates(data_variants.without_key, rb, data_variants.aggregates_pool);
    if (data_variants.type == AggregatedDataVariants::Type::without_key)
        return;

    /// Data variants is inited with single level hashmap, however the checkpoint states are 2 levels
    /// which means data variants was converted to two level
    UInt8 is_two_level;
    readIntBinary(is_two_level, rb);
    if (is_two_level && !data_variants.isTwoLevel())
        data_variants.convertToTwoLevel();

    /// [aggr-func-state-in-hash-map]
    if (false)
    {
    } // NOLINT
#define M(NAME, IS_TWO_LEVEL) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        DB::deserializeHashMap(data_variants.NAME->data, [this](auto & mapped, Arena & pool, ReadBuffer & rb_) { deserializeAggregateStates(mapped, rb_, &pool); }, *data_variants.aggregates_pool, rb);

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}

VersionType Aggregator::getVersionFromRevision(UInt64 revision) const
{
    if (revision >= STATE_V3_MIN_REVISION)
        return static_cast<VersionType>(3);
    else if (revision >= STATE_V2_MIN_REVISION)
        return static_cast<VersionType>(2);
    else
        throw Exception(
            ErrorCodes::SERVER_REVISION_IS_TOO_OLD, "State of AggregatedDataVariants is not yet implemented in revision {}", revision);
}

VersionType Aggregator::getVersion() const
{
    return getVersionFromRevision(ProtonRevision::getVersionRevision());
}

auto Aggregator::getZeroOutWindowKeysFunc(Arena * arena) const
{
    auto window_keys_size = params.window_keys_num == 2 ? key_sizes[0] + key_sizes[1] : key_sizes[0];
    bool is_single_key = key_sizes.size() == 1;
    /// In order to merge state with same other keys of different gcd buckets, reset the window group keys to zero
    /// create a new key, where the window key part is 0, and the other key parts are the same as the original value.
    /// For example:
    /// Key :           <window_start> +  <window_end> +  <other_keys...>
    /// New key :                0     +        0      +  <other_keys...>
    return [window_keys_size, arena, is_single_key](const auto & key) {
        if constexpr (std::is_same_v<std::decay_t<decltype(key)>, StringRef>)
        {
            /// Case-1: Serialized key, the window time keys always are always lower bits
            auto * data = const_cast<char *>(arena->insert(key.data, key.size));
            assert(data != nullptr);
            std::memset(data, 0, window_keys_size);

            return SerializedKeyHolder{StringRef(data, key.size), *arena};
        }
        else
        {
            /// Case-2: Only one window time key
            if (is_single_key)
                return static_cast<std::decay_t<decltype(key)>>(0);

            /// Case-3: Fixed key, the fixed window time keys always are always lower bits
            auto bit_nums = window_keys_size << 3;
            return (key >> bit_nums) << bit_nums; /// reset lower bits to zero
        }
    };
}

Block Aggregator::spliceAndConvertToBlock(AggregatedDataVariants & variants, bool final, const std::vector<Int64> & gcd_buckets) const
{
    assert(variants.isTimeBucketTwoLevel());

    bool need_splice = gcd_buckets.size() > 1;
    Arena * arena = variants.aggregates_pool;
    AggregatedDataVariants res;
    if (need_splice)
    {
        res.aggregator = this;
        initDataVariants(res, method_chosen, key_sizes, params);
        arena = res.aggregates_pool;
        assert(res.type == variants.type);
    }

    switch (variants.type)
    {
#define M(NAME) \
        case AggregatedDataVariants::Type::NAME: \
        { \
            if (need_splice) \
            { \
                for (auto bucket : gcd_buckets) \
                    mergeDataImpl<decltype(variants.NAME)::element_type>(res.NAME->data.impls[0], variants.NAME->data.impls[bucket], arena, /*clear_states=*/false, getZeroOutWindowKeysFunc(arena)); \
                return convertOneBucketToBlockImpl(res, *res.NAME, arena, final, /*clear_states=*/true, /*spliced_bucket=*/0); \
            } \
            else \
                return convertOneBucketToBlockImpl(variants, *variants.NAME, arena, final, /*clear_states=*/false, gcd_buckets[0]); \
        }

    APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M
        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    UNREACHABLE();
}

Block Aggregator::mergeAndSpliceAndConvertToBlock(ManyAggregatedDataVariants & variants, bool final, const std::vector<Int64> & gcd_buckets) const
{
    bool need_splice = gcd_buckets.size() > 1;
    auto prepared_data = prepareVariantsToMerge(variants, /*always_merge_into_empty=*/ need_splice);
    if (prepared_data->empty())
        return {};

    auto & first = *prepared_data->at(0);
    assert(first.isTimeBucketTwoLevel());
    Arena * arena = first.aggregates_pool;

    if (false) {} // NOLINT
#define M(NAME) \
    else if (first.type == AggregatedDataVariants::Type::NAME) \
    { \
        using Method = decltype(first.NAME)::element_type; \
        for (auto bucket : gcd_buckets) \
            mergeBucketImpl<Method>(*prepared_data, bucket, arena, /*clear_states=*/ false); \
        if (need_splice) \
        { \
            for (auto bucket : gcd_buckets) \
                mergeDataImpl<Method>(first.NAME->data.impls[0], first.NAME->data.impls[bucket], arena, /*clear_states=*/true, getZeroOutWindowKeysFunc(arena)); \
            return convertOneBucketToBlockImpl(first, *first.NAME, arena, final, /*clear_states=*/ true, /*spliced_bucket=*/0); \
        } \
        else \
            return convertOneBucketToBlockImpl(first, *first.NAME, arena, final, /*clear_states=*/ false, gcd_buckets[0]); \
    }

    APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    UNREACHABLE();
}

template <typename Method>
bool Aggregator::executeAndRetractImpl(
    Method & method,
    Arena * aggregates_pool,
    Arena * retract_pool,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);
    bool need_finalization = false;

    /// NOTE: only row_end-row_start is required, but:
    /// - this affects only optimize_aggregation_in_order,
    /// - this is just a pointer, so it should not be significant,
    /// - and plus this will require other changes in the interface.
    std::unique_ptr<AggregateDataPtr[]> places(new AggregateDataPtr[row_end]);

    /// For all rows.
    for (size_t i = row_begin; i < row_end; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplaceKey(method.data, i, *aggregates_pool);

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.isInserted())
        {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.setMapped(nullptr);

            aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            /// TODO: support use_compiled_functions
            createAggregateStates(aggregate_data);
            emplace_result.setMapped(aggregate_data);
        }
        else
        {
            aggregate_data = emplace_result.getMapped();

            /// Save changed group with retracted state (used for emit changed group)
            /// If there are aggregate data and no retracted data, copy aggregate data to retracted data before changed
            if (!TrackingUpdates::empty(aggregate_data) && !TrackingUpdatesWithRetract::hasRetract(aggregate_data))
            {
                auto & retract_data = TrackingUpdatesWithRetract::getRetract(aggregate_data);
                auto tmp_retract = retract_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(tmp_retract, /*prefix_with_updates_tracking_state=*/ false);
                retract_data = tmp_retract;
                mergeAggregateStates(retract_data, aggregate_data, retract_pool, /*clear_states=*/ false);
            }
        }

        assert(aggregate_data != nullptr);
        places[i] = aggregate_data;
    }

    /// Add values to the aggregate functions.
    for (size_t i = 0; i < aggregate_functions.size(); ++i)
    {
        AggregateFunctionInstruction * inst = aggregate_instructions + i;

        if (inst->offsets)
            inst->batch_that->addBatchArray(row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, inst->offsets, aggregates_pool);
        else
            inst->batch_that->addBatch(row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, aggregates_pool, -1, inst->delta_column);

        if (inst->batch_that->isUserDefined())
        {
            AggregateDataPtr * places_ptr = places.get();
            /// It is ok to re-flush if it is flush already, then we don't need maintain a map to check if it is ready flushed
            for (size_t j = row_begin; j < row_end; ++j)
            {
                if (places_ptr[j])
                {
                    inst->batch_that->flush(places_ptr[j] + inst->state_offset);
                    if (!need_finalization)
                        need_finalization = (inst->batch_that->getEmitTimes(places_ptr[j] + inst->state_offset) > 0);
                }
            }
        }
    }

    if (needTrackUpdates())
        TrackingUpdates::addBatch(row_begin, row_end, places.get(), aggregate_instructions ? aggregate_instructions->delta_column : nullptr);

    return need_finalization;
}

std::pair<bool, bool> Aggregator::executeAndRetractOnBlock(
    Columns columns,
    size_t row_begin,
    size_t row_end,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns) const
{
    std::pair<bool, bool> return_result = {false, false};
    auto & need_abort = return_result.first;
    auto & need_finalization = return_result.second;

    if (unlikely(row_end <= row_begin))
        return return_result;

    result.aggregator = this;
    if (result.empty())
    {
        initDataVariants(result, method_chosen, key_sizes, params);
        initStatesForWithoutKey(result);
        LOG_TRACE(log, "Aggregation method: {}", result.getMethodName());
    }

    Columns materialized_columns = materializeKeyColumns(columns, key_columns, params, result.isLowCardinality());

    setupAggregatesPoolTimestamps(row_begin, row_end, key_columns, result.aggregates_pool);

    NestedColumnsHolder nested_columns_holder;
    AggregateFunctionInstructions aggregate_functions_instructions;
    prepareAggregateInstructions(columns, aggregate_columns, materialized_columns, aggregate_functions_instructions, nested_columns_holder);

    assert(trackingUpdatesType() == TrackingUpdatesType::UpdatesWithRetract);
    if (result.type == AggregatedDataVariants::Type::without_key)
    {
        /// Save last finalization state into `retracted_result` before processing new data.
        /// We shall clear and reset it after finalization
        if (!TrackingUpdates::empty(result.without_key) && !TrackingUpdatesWithRetract::hasRetract(result.without_key))
        {
            auto & retract_data = TrackingUpdatesWithRetract::getRetract(result.without_key);
            auto aggregate_data = result.retract_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
            createAggregateStates(aggregate_data, /*prefix_with_updates_tracking_state=*/ false);
            retract_data = aggregate_data;
            mergeAggregateStates(retract_data, result.without_key, result.retract_pool.get(), /*clear_states=*/ false);
        }

        need_finalization = executeWithoutKeyImpl(
            result.without_key, row_begin, row_end, aggregate_functions_instructions.data(), result.aggregates_pool);
    }

#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == AggregatedDataVariants::Type::NAME) need_finalization = executeAndRetractImpl( \
        *result.NAME, \
        result.aggregates_pool, \
        result.retract_pool.get(), \
        row_begin, \
        row_end, \
        key_columns, \
        aggregate_functions_instructions.data());

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M

    need_abort = checkAndProcessResult(result);
    return return_result;
}

void Aggregator::mergeAggregateStates(AggregateDataPtr & dst, AggregateDataPtr & src, Arena * arena, bool clear_states) const
{
    assert(src);
    assert(dst);

    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_functions[i]->merge(dst + offsets_of_aggregate_states[i], src + offsets_of_aggregate_states[i], arena);

    if (clear_states)
        destroyAggregateStates(src);
}

void Aggregator::destroyAggregateStates(AggregateDataPtr & place) const
{
    if (place)
    {
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(place + offsets_of_aggregate_states[i]);

        place = nullptr;
    }
}

void Aggregator::serializeAggregateStates(const AggregateDataPtr & place, WriteBuffer & wb) const
{
    UInt8 has_states = place ? 1 : 0;
    writeIntBinary(has_states, wb);
    if (has_states)
    {
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->serialize(place + offsets_of_aggregate_states[i], wb);
    }
}

void Aggregator::deserializeAggregateStates(AggregateDataPtr & place, ReadBuffer & rb, Arena * arena) const
{
    /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
    place = nullptr;

    UInt8 has_states;
    readIntBinary(has_states, rb);
    if (has_states)
    {
        /// Allocate states for all aggregate functions
        AggregateDataPtr aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(aggregate_data);
        place = aggregate_data;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->deserialize(place + offsets_of_aggregate_states[i], rb, std::nullopt, arena);
    }
}

void Aggregator::doCheckpointV3(const AggregatedDataVariants & data_variants, WriteBuffer & wb) const
{
    /// Serialization layout, there are 2 cases:
    /// 1) Without key: [uint8][uint16][aggr-func-state-without-key]
    /// 2) Otherwise: [uint8][uint16][aggr-func-state-for-overflow-row][is_two_level][aggr-func-state-in-hash-map]
    bool inited = !data_variants.empty();
    writeBinary(inited, wb);
    if (!inited)
        return; /// No aggregated data yet

    writeIntBinary<UInt8>(static_cast<UInt8>(data_variants.type), wb);

    writeIntBinary<UInt8>(static_cast<UInt8>(trackingUpdatesType()), wb);

    auto state_serializer = [this](auto place, auto & wb_) {
        assert(place);
        if (trackingUpdatesType() == TrackingUpdatesType::UpdatesWithRetract)
        {
            TrackingUpdates::serialize(place, wb_);

             auto retract_place = TrackingUpdatesWithRetract::getRetract(place);
             bool has_retract = retract_place != nullptr;
             writeBinary(has_retract, wb_);
             if (has_retract)
                 for (size_t i = 0; i < params.aggregates_size; ++i)
                     aggregate_functions[i]->serialize(retract_place + offsets_of_aggregate_states[i], wb_);
        }
        else if (trackingUpdatesType() == TrackingUpdatesType::Updates)
            TrackingUpdates::serialize(place, wb_);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->serialize(place + offsets_of_aggregate_states[i], wb_);
    };

    /// [aggr-func-state-without-key]
    if (data_variants.type == AggregatedDataVariants::Type::without_key)
        state_serializer(data_variants.without_key, wb);

    /// [aggr-func-state-in-hash-map]
#define M(NAME, IS_TWO_LEVEL) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
    { \
        if constexpr (IS_TWO_LEVEL) \
            DB::serializeTwoLevelHashMap(data_variants.NAME->data, [&](const auto & mapped, WriteBuffer & wb_) { state_serializer(mapped, wb_); }, wb); \
        else \
            DB::serializeHashMap(data_variants.NAME->data, [&](const auto & mapped, WriteBuffer & wb_) { state_serializer(mapped, wb_); }, wb); \
    }

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}

void Aggregator::doRecoverV3(AggregatedDataVariants & data_variants, ReadBuffer & rb) const
{
    bool inited = !data_variants.empty();
    readBinary(inited, rb);
    if (!inited)
        return;

    UInt8 recovered_data_variants_type_uint8;
    readIntBinary<UInt8>(recovered_data_variants_type_uint8, rb);
    AggregatedDataVariants::Type recovered_data_variants_type = static_cast<AggregatedDataVariants::Type>(recovered_data_variants_type_uint8);

    data_variants.aggregator = this;
    initDataVariants(data_variants, method_chosen, key_sizes, params);
    /// Data variants is inited with single level hashmap, however the checkpoint states are 2 levels
    /// which means data variants was converted to two level
    if (data_variants.type != recovered_data_variants_type)
        if (data_variants.isConvertibleToTwoLevel())
            data_variants.convertToTwoLevel();

    if (data_variants.type != recovered_data_variants_type)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover aggregation checkpoint. Aggregated data variant type is not compatible, checkpointed={}, current={}",
            magic_enum::enum_name(recovered_data_variants_type),
            magic_enum::enum_name(method_chosen));

    UInt8 recovered_expanded_data_type_uint8;
    readIntBinary<UInt8>(recovered_expanded_data_type_uint8, rb);
    TrackingUpdatesType recovered_expanded_data_type = static_cast<TrackingUpdatesType>(recovered_expanded_data_type_uint8);
    if (recovered_expanded_data_type != trackingUpdatesType())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover aggregation checkpoint. Expanded data type is not the same, checkpointed={}, current={}",
            magic_enum::enum_name(recovered_expanded_data_type),
            magic_enum::enum_name(trackingUpdatesType()));

    auto state_deserializer = [this, &data_variants](auto & place, auto & rb_, Arena *) {
        place = nullptr; /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
        auto aggregate_data = data_variants.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(aggregate_data);
        place = aggregate_data;

        if (trackingUpdatesType() == TrackingUpdatesType::UpdatesWithRetract)
        {
            TrackingUpdates::deserialize(place, rb_);

            auto & retract_place = TrackingUpdatesWithRetract::getRetract(place);
            bool has_retract = false;
            readBinary(has_retract, rb_);
            if (has_retract)
            {
                auto tmp_retract = data_variants.retract_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(tmp_retract, /*prefix_with_updates_tracking_state=*/ false);
                retract_place = tmp_retract;
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->deserialize(retract_place + offsets_of_aggregate_states[i], rb_, std::nullopt, data_variants.retract_pool.get());
            }
        }
        else if (trackingUpdatesType() == TrackingUpdatesType::Updates)
            TrackingUpdates::deserialize(place, rb_);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->deserialize(place + offsets_of_aggregate_states[i], rb_, std::nullopt, data_variants.aggregates_pool);
    };

    /// [aggr-func-state-without-key]
    if (data_variants.type == AggregatedDataVariants::Type::without_key)
        state_deserializer(data_variants.without_key, rb, data_variants.aggregates_pool);

    /// [aggr-func-state-in-hash-map]
#define M(NAME, IS_TWO_LEVEL) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
    { \
        if constexpr (IS_TWO_LEVEL) \
            DB::deserializeTwoLevelHashMap(data_variants.NAME->data, [&](auto & mapped, Arena & pool, ReadBuffer & rb_) { state_deserializer(mapped, rb_, &pool); }, *data_variants.aggregates_pool, rb); \
        else \
            DB::deserializeHashMap(data_variants.NAME->data, [&](auto & mapped, Arena & pool, ReadBuffer & rb_) { state_deserializer(mapped, rb_, &pool); }, *data_variants.aggregates_pool, rb); \
    }

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}

bool Aggregator::checkAndProcessResult(AggregatedDataVariants & result) const
{
    size_t result_size = result.sizeWithoutOverflowRow();
    Int64 current_memory_usage = 0;
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            current_memory_usage = memory_tracker->get();

    /// Here all the results in the sum are taken into account, from different threads.
    Int64 result_size_bytes = current_memory_usage - memory_usage_before_aggregation;

    bool worth_convert_to_two_level = worthConvertToTwoLevel(
        params.group_by_two_level_threshold, result_size, params.group_by_two_level_threshold_bytes, result_size_bytes);

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
    if (params.max_bytes_before_external_group_by
        && result.isTwoLevel()
        && current_memory_usage > static_cast<Int64>(params.max_bytes_before_external_group_by)
        && worth_convert_to_two_level)
    {
        size_t size = current_memory_usage + params.min_free_disk_space;

        std::string tmp_path = params.tmp_volume->getDisk()->getPath();

        // enoughSpaceInDirectory() is not enough to make it right, since
        // another process (or another thread of aggregator) can consume all
        // space.
        //
        // But true reservation (IVolume::reserve()) cannot be used here since
        // current_memory_usage does not take compression into account and
        // will reserve way more that actually will be used.
        //
        // Hence, let's do a simple check.
        if (!enoughSpaceInDirectory(tmp_path, size))
            throw Exception("Not enough space for external aggregation in " + tmp_path, ErrorCodes::NOT_ENOUGH_SPACE);

        writeToTemporaryFile(result, tmp_path);
    }

    return false;
}

BlocksList Aggregator::convertUpdatesToBlocks(AggregatedDataVariants & data_variants) const
{
    LOG_DEBUG(log, "Converting updated aggregated data to blocks");

    Stopwatch watch;

    BlocksList blocks;

    /// In what data structure is the data aggregated?
    if (data_variants.empty())
        return blocks;

    constexpr bool final = true;
    constexpr bool clear_states = false;
    if (data_variants.type == AggregatedDataVariants::Type::without_key)
        blocks.emplace_back(prepareBlockAndFillWithoutKey(data_variants, final, clear_states, ConvertType::Updates));
    else if (!data_variants.isTwoLevel())
        blocks = prepareBlockAndFillSingleLevel(data_variants, final, clear_states, ConvertType::Updates);
    else
        blocks = prepareBlocksAndFillTwoLevel(data_variants, final, clear_states, /*max_threads=*/ 1, ConvertType::Updates);

    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & block : blocks)
    {
        rows += block.rows();
        bytes += block.bytes();
    }

    double elapsed_seconds = watch.elapsedSeconds();
    LOG_DEBUG(log,
        "Converted updated aggregated data to blocks. {} rows, {} in {} sec. ({:.3f} rows/sec., {}/sec.)",
        rows, ReadableSize(bytes),
        elapsed_seconds, rows / elapsed_seconds,
        ReadableSize(bytes / elapsed_seconds));

    return blocks;
}


template <bool is_two_level, typename Table, typename KeyHandler>
void NO_INLINE Aggregator::mergeUpdatesDataImpl(std::vector<Table *> & tables, Arena * arena, bool reset_updated, KeyHandler && key_handler) const
{
    assert(!tables.empty());
    auto & dst_table = *tables.front();
    /// Always merge updated data into empty first.
    assert(dst_table.empty());

    /// For example:
    ///                 thread-1        thread-2
    ///     group-1     updated       non-updated
    ///     group-2     non-updated   updated
    ///     group-3     non-updated   non-updated
    ///
    /// 1) Collect all updated groups
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
                auto aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(aggregate_data, /*prefix_with_updates_tracking_state=*/ false);
                dst = aggregate_data;
            }

            if (reset_updated)
                TrackingUpdates::resetUpdated(mapped);
        };

        if constexpr (is_two_level)
            src_table.forEachValueOfUpdatedBuckets(std::move(merge_updated_func), reset_updated);
        else
            src_table.forEachValue(std::move(merge_updated_func));
    }

    /// 2) Merge all updated groups parts for each thread (based on `1)` )
    /// `dst` <= (thread-1: group-1  group-2) + (thread-2: group-1 group-2)
    for (size_t i = 1; i < tables.size(); ++i)
    {
        auto & src_table = *tables[i];

        if constexpr (std::is_same_v<KeyHandler, EmptyKeyHandler>)
            /// a. If use original key, we only loop updated groups `dst_table`, then find and merge states from `src_table`
            dst_table.forEachValue([&](const auto & key, auto & mapped) {
                auto find_it = src_table.find(key);
                if (!find_it)
                    return;

                mergeAggregateStates(mapped, find_it->getMapped(), arena, /*clear_states=*/ false);
            });
        else
            /// b. If use handled key (e.g. reset window keys to zero), we loop all groups of `src_table`, then find and merge states of update groups from `dst_table` via handled key
            src_table.forEachValue([&](const auto & key, auto & mapped) {
                auto key_holder = key_handler(key);
                auto find_it = dst_table.find(keyHolderGetKey(key_holder));
                if (!find_it)
                    return;

                mergeAggregateStates(find_it->getMapped(), mapped, arena, /*clear_states=*/ false);
            });
    }
}

Block Aggregator::spliceAndConvertUpdatesToBlock(AggregatedDataVariants & variants, const std::vector<Int64> & gcd_buckets) const
{
    assert(variants.isTimeBucketTwoLevel());
    assert(needTrackUpdates());

    AggregatedDataVariants res;
    res.aggregator = this;
    initDataVariants(res, method_chosen, key_sizes, params);
    assert(res.type == variants.type);

    Arena * arena = res.aggregates_pool;
    constexpr bool final = true;
    constexpr bool clear_states = true; /// Always clear tmp spliced states
    constexpr bool reset_updated = false; /// Don't reset updated, there may be overlapping gcd buckets
    bool need_splice = gcd_buckets.size() > 1;

    switch (variants.type)
    {
#define M(NAME) \
        case AggregatedDataVariants::Type::NAME: \
        { \
            using Table = std::decay_t<decltype(variants.NAME->data.impls[0])>; \
            std::vector<Table *> tables = {nullptr}; \
            tables.reserve(gcd_buckets.size() + 1); \
            for (auto bucket : gcd_buckets) \
                tables.emplace_back(&variants.NAME->data.impls[bucket]); \
            if (need_splice) \
            { \
                tables[0] = &(res.NAME->data.impls[0]); \
                mergeUpdatesDataImpl</*is_two_level=*/false>(tables, arena, reset_updated, getZeroOutWindowKeysFunc(arena)); \
                return convertOneBucketToBlockImpl(res, *res.NAME, arena, final, clear_states, /*splicted_bucket=*/0, ConvertType::Normal); \
            } \
            else \
            { \
                tables[0] = &(res.NAME->data.impls[gcd_buckets[0]]); \
                mergeUpdatesDataImpl</*is_two_level=*/false>(tables, arena, reset_updated); \
                return convertOneBucketToBlockImpl(res, *res.NAME, arena, final, clear_states, gcd_buckets[0], ConvertType::Normal); \
            } \
        }

    APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M
        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    UNREACHABLE();
}

Block Aggregator::mergeAndSpliceAndConvertUpdatesToBlock(ManyAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const
{
    auto prepared_data_ptr = prepareVariantsToMerge(data_variants, /*always_merge_into_empty=*/ true);
    if (prepared_data_ptr->empty())
        return {};

    auto & first = *prepared_data_ptr->at(0);
    assert(first.isTimeBucketTwoLevel());
    assert(needTrackUpdates());

    Arena * arena = first.aggregates_pool;
    constexpr bool final = true;
    constexpr bool clear_states = true; /// Always clear tmp spliced states
    constexpr bool reset_updated = false; /// Don't reset updated, there may be overlapping gcd buckets
    bool need_splice = gcd_buckets.size() > 1;
    ManyAggregatedDataVariants spliced_variants;

    switch (first.type)
    {
#define M(NAME) \
        case AggregatedDataVariants::Type::NAME: \
        { \
            using Table = std::decay_t<decltype(first.NAME->data.impls[0])>; \
            std::vector<Table *> tables {nullptr}; \
            tables.reserve(gcd_buckets.size() * (prepared_data_ptr->size() - 1) + 1); \
            for (auto bucket : gcd_buckets) \
                for (size_t i = 1; i < prepared_data_ptr->size(); ++i) \
                    tables.emplace_back(&(prepared_data_ptr->at(i)->NAME->data.impls[bucket])); \
            if (need_splice) \
            { \
                tables[0] = &(first.NAME->data.impls[0]); \
                mergeUpdatesDataImpl</*is_two_level=*/false>(tables, arena, reset_updated, getZeroOutWindowKeysFunc(arena)); \
                return convertOneBucketToBlockImpl(first, *first.NAME, arena, final, clear_states, /*bucket=*/0, ConvertType::Normal); \
            } \
            else \
            { \
                tables[0] = &(first.NAME->data.impls[gcd_buckets[0]]); \
                mergeUpdatesDataImpl</*is_two_level=*/false>(tables, arena, reset_updated); \
                return convertOneBucketToBlockImpl(first, *first.NAME, arena, final, clear_states, gcd_buckets[0], ConvertType::Normal); \
            } \
        }

    APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL(M)
#undef M
        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    UNREACHABLE();
}

void Aggregator::resetUpdatedForBuckets(AggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const
{
    if (data_variants.empty())
        return;

    assert(data_variants.isTimeBucketTwoLevel());
    switch (data_variants.type)
    {
#define M(NAME) \
        case AggregatedDataVariants::Type::NAME: \
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
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    UNREACHABLE();
}

AggregatedDataVariantsPtr Aggregator::mergeUpdateGroups(ManyAggregatedDataVariants & data_variants) const
{
    auto prepared_data_ptr = prepareVariantsToMerge(data_variants, /*always_merge_into_empty=*/ true);
    if (prepared_data_ptr->empty())
        return {};

    BlocksList blocks;
    auto & first = *prepared_data_ptr->at(0);
    switch (first.type)
    {
        case AggregatedDataVariants::Type::without_key:
        {
            if (std::ranges::none_of(*prepared_data_ptr, [](auto & variants) {
                    return variants->without_key && TrackingUpdates::updated(variants->without_key);
                }))
                return {};

            mergeWithoutKeyDataImpl(*prepared_data_ptr, /*clear_states=*/ false);
            break;
        }

#define M(NAME, IS_TWO_LEVEL) \
        case AggregatedDataVariants::Type::NAME: \
        { \
            using Table = std::decay_t<decltype(first.NAME->data)>; \
            std::vector<Table *> tables; \
            tables.reserve(prepared_data_ptr->size()); \
            for (auto & variants : *prepared_data_ptr) \
                tables.emplace_back(&(variants->NAME->data)); \
            mergeUpdatesDataImpl<IS_TWO_LEVEL>(tables, first.aggregates_pool, /*reset_updated*/ true); \
            break; \
        }

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
        default:
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    return prepared_data_ptr->at(0);
}

BlocksList Aggregator::convertRetractToBlocks(AggregatedDataVariants & data_variants) const
{
    LOG_DEBUG(log, "Converting retract aggregated data to blocks");

    Stopwatch watch;

    BlocksList blocks;

    if (data_variants.empty())
        return blocks;

    constexpr bool final = true;
    constexpr bool clear_states = true;
    if (data_variants.type == AggregatedDataVariants::Type::without_key)
        blocks.emplace_back(prepareBlockAndFillWithoutKey(data_variants, final, clear_states, ConvertType::Retract));
    else if (!data_variants.isTwoLevel())
        blocks = prepareBlockAndFillSingleLevel(data_variants, final, clear_states, ConvertType::Retract);
    else
        blocks = prepareBlocksAndFillTwoLevel(data_variants, final, clear_states, /*max_threads=*/ 1, ConvertType::Retract);

    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & block : blocks)
    {
        rows += block.rows();
        bytes += block.bytes();
    }

    double elapsed_seconds = watch.elapsedSeconds();
    LOG_DEBUG(log,
        "Converted retract aggregated data to blocks. {} rows, {} in {} sec. ({:.3f} rows/sec., {}/sec.)",
        rows, ReadableSize(bytes),
        elapsed_seconds, rows / elapsed_seconds,
        ReadableSize(bytes / elapsed_seconds));

    return blocks;
}

template <typename Method>
void Aggregator::mergeRetractGroupsImpl(ManyAggregatedDataVariants & non_empty_data, Arena * arena) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];
    auto & dst_table = getDataVariant<Method>(*res).data;
    /// Always merge retract data into empty first.
    assert(dst_table.empty());

    /// For example:
    ///                 thread-1        thread-2
    ///     group-1     retract       non-retract
    ///     group-2     non-retract   retract
    ///     group-3     non-retract   non-retract
    ///
    /// 1) Collect all retract groups
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
                auto aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(aggregate_data, /*prefix_with_updates_tracking_state=*/ false);
                dst = aggregate_data;
            }
        });
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
                    mergeAggregateStates(mapped, TrackingUpdatesWithRetract::getRetract(src_mapped), arena, /*clear_states=*/ true);
                else
                    /// If retract data not exist, assume it does't be changed, we should use original data
                    mergeAggregateStates(mapped, src_mapped, arena, /*clear_states=*/ false);
            }
        });

        current.resetAndCreateRetractPool();
    }
}

AggregatedDataVariantsPtr Aggregator::mergeRetractGroups(ManyAggregatedDataVariants & data_variants) const
{
    auto prepared_data_ptr = prepareVariantsToMerge(data_variants, /*always_merge_into_empty=*/ true);
    if (prepared_data_ptr->empty())
        return {};

    if (unlikely(params.overflow_row))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Overflow row processing is not implemented in streaming aggregation");

    auto & first = *prepared_data_ptr->at(0);
    if (first.type == AggregatedDataVariants::Type::without_key)
    {
        if (std::ranges::none_of(*prepared_data_ptr, [](auto & variants) { return TrackingUpdatesWithRetract::hasRetract(variants->without_key); }))
            return {}; /// Skip if no retracted

        for (size_t result_num = 1, size = prepared_data_ptr->size(); result_num < size; ++result_num)
        {
            auto & current = *(*prepared_data_ptr)[result_num];
            if (TrackingUpdatesWithRetract::hasRetract(current.without_key))
                mergeAggregateStates(
                    first.without_key, TrackingUpdatesWithRetract::getRetract(current.without_key), first.aggregates_pool, /*clear_states=*/ true);
            else
                /// If retract data not exist, assume it does't be changed, we should use original data
                mergeAggregateStates(first.without_key, current.without_key, first.aggregates_pool, /*clear_states=*/ false);

            current.resetAndCreateRetractPool();
        }
    }

#define M(NAME, IS_TWO_LEVEL) \
    else if (first.type == AggregatedDataVariants::Type::NAME) \
        mergeRetractGroupsImpl<decltype(first.NAME)::element_type>(*prepared_data_ptr, first.aggregates_pool);

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    return prepared_data_ptr->at(0);
}

void Aggregator::updateMetrics(const AggregatedDataVariants & variants, AggregatedDataMetrics & metrics) const
{
    switch (variants.type)
    {
        case AggregatedDataVariants::Type::EMPTY: break;
        case AggregatedDataVariants::Type::without_key:
        {
            metrics.total_aggregated_rows += 1;
            metrics.total_bytes_of_aggregate_states += total_size_of_aggregate_states;
            break;
        }

    #define M(NAME, IS_TWO_LEVEL) \
        case AggregatedDataVariants::Type::NAME: \
        { \
            auto & table = variants.NAME->data; \
            metrics.total_aggregated_rows += table.size(); \
            metrics.hash_buffer_bytes += table.getBufferSizeInBytes(); \
            metrics.hash_buffer_cells += table.getBufferSizeInCells(); \
            metrics.total_bytes_of_aggregate_states += table.size() * total_size_of_aggregate_states; \
            break; \
        }

        APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
    #undef M
    }

    for (const auto & arena : variants.aggregates_pools)
        metrics.total_bytes_in_arena += arena->size();
}
/// proton: ends
}
}
