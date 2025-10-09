#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingCount.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingTime.h>
#include <Interpreters/Streaming/ChooseHybridHashMethod.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{

HybridAggregator::HybridAggregator(const Block & input_header_, const HybridAggregatorParamsPtr & hybrid_params_)
    : IAggregator(hybrid_params_, input_header_, "HybridAggregator"), hybrid_params(hybrid_params_)
{
    /// Use query-level memory tracker
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
    {
        if (auto * memory_tracker = memory_tracker_child->getParent())
            memory_usage_before_aggregation = memory_tracker->get();
    }

    calculateAggregateStateSizes();

    DataTypes key_col_types;
    key_col_types.reserve(keys_positions.size());
    for (auto key_pos : keys_positions)
        key_col_types.push_back(input_header.getByPosition(key_pos).type);

    bool needs_time_bucket
        = (params->group_by == IAggregatorParams::GroupBy::WindowStart || params->group_by == IAggregatorParams::GroupBy::WindowEnd);

    auto hash_method = chooseHybridHashMethod(key_col_types, needs_time_bucket);
    method_chosen = hash_method.type;
    key_sizes.swap(hash_method.key_sizes);
    bucket_key_offset = hash_method.bucket_key_offset;
    has_nullable_key = hash_method.has_nullable_key;

    if (params->emit_key_params)
    {
        /// Only support string key for now
        switch (method_chosen)
        {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        break;
            APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M)
#undef M
            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "`EMIT AFTER KEY EXPIRE` only supports {} hash key", method_chosen);
        }
    }
}

void HybridAggregator::calculateAggregateStateSizes()
{
    aggregate_functions.resize(params->aggregates_size);
    for (size_t i = 0; i < params->aggregates_size; ++i)
        aggregate_functions[i] = params->aggregates[i].function.get();

    /// Initialize sizes of aggregation states and its offsets.
    offsets_of_aggregate_states.resize(params->aggregates_size);
    total_size_of_aggregate_states = 0;
    all_aggregates_has_trivial_destructor = true;

    /// aggregate_states will be aligned as below if emit aggregation is `AFTER KEY EXPIRE`:
    ///  |<-- TrackingCount/TrackingTime -->|<-- state_1 -->|<-- pad_1 -->|<-- state_2 -->|<-- pad_2 -->| .....
    /// otherwise it will be like this
    /// |<-- state_1 -->|<-- pad_1 -->|<-- state_2 -->|<-- pad_2 -->| .....
    ///
    /// pad_N will be used to match alignment requirement for each next state.
    /// The address of state_1 is aligned based on maximum alignment requirements in states
    if (trackingStateCount())
    {
        tracking_count_offset = total_size_of_aggregate_states;
        total_size_of_aggregate_states += sizeof(TrackingCount);
        align_aggregate_states = std::max(align_aggregate_states, alignof(TrackingCount));
    }

    if (trackingStateTime())
    {
        tracking_time_offset = total_size_of_aggregate_states;
        total_size_of_aggregate_states += sizeof(TrackingTime);
        align_aggregate_states = std::max(align_aggregate_states, alignof(TrackingTime));
    }

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

        if (params->aggregates[i].function->allocatesMemoryInArena())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "{} is not supported yet in HybridAggregator since it allocates memory in arena",
                params->aggregates[i].function->getName());
    }
}

std::vector<Int64> HybridAggregator::buckets(const IAggregatedDataVariants & variants_result) const
{
    assert(variants_result.aggregatorType() == AggregatorType::Hybrid);
    const auto & result = static_cast<const HybridAggregatedDataVariants &>(variants_result);
    if (variants_result.empty())
        return {};

    switch (method_chosen)
    {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
        return result.table.NAME->buckets();

        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)
#undef M

        default:
            break;
    }
    return {};
}

void HybridAggregator::removeBucketsBefore(IAggregatedDataVariants & variants_result, Int64 max_bucket, UInt64 transform_id) const
{
    assert(variants_result.aggregatorType() == AggregatorType::Hybrid);
    auto & result = static_cast<HybridAggregatedDataVariants &>(variants_result);

    if (result.empty())
        return;

    size_t removed = 0;
    Int64 last_removed_time_bucket = 0;
    size_t remaining = 0;

    switch (method_chosen)
    {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        std::tie(removed, last_removed_time_bucket, remaining) = result.table.NAME->removeBucketsBefore(max_bucket); \
        if (result.updates.NAME) \
            result.updates.NAME->removeBucketsBefore(max_bucket); \
        break; \
    }

        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)
#undef M

        default:
            break;
    }

    LOG_INFO(
        logger,
        "max_bucket={} removed_buckets={} last_removed_time_bucket={} remaining_buckets={}",
        max_bucket,
        removed,
        last_removed_time_bucket,
        remaining);
}

void HybridAggregator::resetUpdatedForBuckets(IAggregatedDataVariants & variants, const std::vector<Int64> & gcd_buckets) const
{
    assert(variants.aggregatorType() == AggregatorType::Hybrid);
    auto & data_variants = static_cast<HybridAggregatedDataVariants &>(variants);

    switch (method_chosen)
    {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        auto * updates = data_variants.updates.NAME.get(); \
        if (!updates) \
            return; \
\
        auto many_bucket_updates = updates->bucketTables(gcd_buckets); \
        for (auto [_, bucket_updates] : many_bucket_updates) \
            bucket_updates->clear(); \
\
        break; \
    }

        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)
#undef M

        default:
            break;
    }
}

}

}
