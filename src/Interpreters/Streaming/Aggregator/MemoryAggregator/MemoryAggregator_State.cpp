#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>

namespace DB::Streaming
{

template <typename Method, typename Table>
void NO_INLINE MemoryAggregator::destroyImpl(Table & table) const
{
    table.forEachMapped([&](AggregateDataPtr & data) {
        /** If an exception (usually a lack of memory, the MemoryTracker throws) arose
          *  after inserting the key into a hash table, but before creating all states of aggregate functions,
          *  then data will be equal nullptr.
          */
        destroyAggregateStates(data);
    });
}

void MemoryAggregator::destroyAggregateStates(AggregateDataPtr & place) const
{
    doDestroyAggregateStates(place);
    place = nullptr;
}

void MemoryAggregator::serializeAggregateStates(const AggregateDataPtr & place, WriteBuffer & wb) const
{
    chassert(place);

    for (size_t i = 0; i < params->aggregates_size; ++i)
        aggregate_functions[i]->serialize(place + offsets_of_aggregate_states[i], wb);
}

void MemoryAggregator::deserializeAggregateStates(AggregateDataPtr & place, ReadBuffer & rb, Arena * arena) const
{
    chassert(place);

    for (size_t i = 0; i < params->aggregates_size; ++i)
        aggregate_functions[i]->deserialize(place + offsets_of_aggregate_states[i], rb, std::nullopt, arena);
}


void MemoryAggregator::createAggregateStates(AggregateDataPtr & aggregate_data, bool skip_compiled_aggregate_functions) const
{
    /// Initialize reserved TrackingUpdates
    chassert(aggregate_data);
    switch (params->tracking_updates_type)
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

    for (size_t j = 0; j < params->aggregates_size; ++j)
    {
        if (skip_compiled_aggregate_functions && is_aggregate_function_compiled[j])
            continue;

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

void MemoryAggregator::destroyWithoutKey(MemoryAggregatedDataVariants & result) const
{
    destroyAggregateStates(result.without_key);
}

void MemoryAggregator::destroyAllAggregateStates(MemoryAggregatedDataVariants & result) const
{
    if (result.empty())
        return;

    LOG_TRACE(logger, "Destroying aggregate states");

    /// In what data structure is the data aggregated?
    if (result.type == MemoryAggregatedDataVariants::Type::without_key)
        destroyWithoutKey(result);

#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        destroyImpl<decltype(result.NAME)::element_type>(result.NAME->data); \
    }

    if (false)
    {
    } // NOLINT
    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else if (result.type != MemoryAggregatedDataVariants::Type::without_key)
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }
}

void MemoryAggregator::initStatesForWithoutKey(MemoryAggregatedDataVariants & data_variants) const
{
    if (!data_variants.without_key && data_variants.type == MemoryAggregatedDataVariants::Type::without_key)
    {
        AggregateDataPtr place = data_variants.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        data_variants.without_key = place;
    }
}

}
