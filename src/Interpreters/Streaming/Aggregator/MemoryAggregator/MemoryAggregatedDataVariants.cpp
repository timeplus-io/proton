#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregatedDataVariants.h>
#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>

#include <Common/HashMapsTemplate.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int RECOVER_CHECKPOINT_FAILED;
extern const int BAD_VERSION;
}

namespace Streaming
{
MemoryAggregatedDataVariants::MemoryAggregatedDataVariants(bool enable_recycle)
    : aggregates_pools(1, std::make_shared<Arena>()), aggregates_pool(aggregates_pools.back().get())
{
    aggregates_pool->enableRecycle(enable_recycle);
}

MemoryAggregatedDataVariants::~MemoryAggregatedDataVariants()
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

void MemoryAggregatedDataVariants::reset()
{
    assert(aggregator);

    /// Clear states
    if (!aggregator->all_aggregates_has_trivial_destructor)
        aggregator->destroyAllAggregateStates(*this);

    /// Clear hash map
    switch (type)
    {
        case MemoryAggregatedDataVariants::Type::EMPTY:
            break;
        case MemoryAggregatedDataVariants::Type::without_key:
            without_key = nullptr;
            break;

#define M(NAME, IS_TWO_LEVEL) \
    case MemoryAggregatedDataVariants::Type::NAME: \
        NAME.reset(); \
        break;
            APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    }
    invalidate();

    /// Reset pool
    resetAndCreateAggregatesPools();
    retract_pool.reset();
}

void MemoryAggregatedDataVariants::convertToTwoLevel()
{
    if (aggregator)
        LOG_INFO(aggregator->logger, "Converting aggregation data to two-level.");

    switch (type)
    {
#define M(NAME) \
    case Type::NAME: \
        NAME##_two_level = std::make_unique<decltype(NAME##_two_level)::element_type>(*(NAME)); \
        (NAME).reset(); \
        type = Type::NAME##_two_level; \
        break;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_STATIC_BUCKET_TWO_LEVEL(M)

#undef M

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong data variant passed.");
    }
}

std::tuple<size_t, size_t> MemoryAggregatedDataVariants::removeBucketsBefore(Int64 max_bucket)
{
    std::pair<size_t, size_t> result = {0, 0};
    for (auto iter = time_bucket_arenas.begin(); iter != time_bucket_arenas.end();)
    {
        //        LOG_INFO(
        //            aggregator->logger,
        //            "arena_bucket={} arena_size={} chunks={} head_chunk_size={}",
        //            iter->first,
        //            iter->second->size(),
        //            iter->second->numOfChunks(),
        //            iter->second->headChunkSize());
        //
        if (iter->first <= max_bucket)
        {
            ++result.first;
            result.second += iter->second->size();
            iter = time_bucket_arenas.erase(iter);
        }
        else
        {
            ++iter;
            /// break;
        }
    }

    return result;
}

/// The complexity of checkpoint the state of MemoryAggregator is a combination of the following 2 cases
/// 1) The keys can reside in hashmap or in arena
/// 2) The state can reside in arena or in the aggregation function
/// And there is a special one which is group without key
void MemoryAggregatedDataVariants::serialize(WriteBuffer & wb, const IAggregator & aggregator_) const
{
    /// We cannot use itself `aggregator` since if there is no data, it is nullptr.
    chassert(aggregator_.type() == AggregatorType::Memory);
    const auto & memory_aggregator = static_cast<const MemoryAggregator &>(aggregator_);

    /// Serialization layout
    /// [version] + [states layout]
    writeIntBinary(version, wb);

    /// [states layout]: there are 2 cases:
    /// 1) Without key: [inited][type][tracking_type][aggregates_size(V4)][aggr-func-state-without-key]
    /// 2) Otherwise: [inited][type][tracking_type][aggregates_size(V4)][aggr-func-state-for-overflow-row][is_two_level][aggr-func-state-in-hash-map]
    bool inited = !empty();
    writeBinary(inited, wb);
    if (!inited)
        return; /// No aggregated data yet

    writeIntBinary<UInt8>(static_cast<UInt8>(type), wb);

    writeIntBinary<UInt8>(static_cast<UInt8>(memory_aggregator.params->tracking_updates_type), wb);

    writeVarUInt(memory_aggregator.params->aggregates.size(), wb); /// Added in V4

    auto state_serializer = [&memory_aggregator](auto place, auto & wb_) {
        chassert(place);
        switch (memory_aggregator.params->tracking_updates_type)
        {
            case TrackingUpdatesType::UpdatesWithRetract:
            {
                TrackingUpdates::serialize(place, wb_);

                bool has_retract = TrackingUpdatesWithRetract::hasRetract(place);
                writeBinary(has_retract, wb_);
                if (has_retract)
                {
                    auto retract_place = TrackingUpdatesWithRetract::getRetract(place);
                    TrackingUpdates::serialize(retract_place, wb_); /// added in V4
                    memory_aggregator.serializeAggregateStates(retract_place, wb_);
                }
                break;
            }
            case TrackingUpdatesType::Updates:
            {
                TrackingUpdates::serialize(place, wb_);
                break;
            }
            default:
                break;
        }

        memory_aggregator.serializeAggregateStates(place, wb_);
    };

    /// [aggr-func-state-without-key]
    if (type == MemoryAggregatedDataVariants::Type::without_key)
        state_serializer(without_key, wb);

    /// [aggr-func-state-in-hash-map]
#define M(NAME, IS_TWO_LEVEL) \
    else if (type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        if constexpr (IS_TWO_LEVEL) \
            DB::serializeTwoLevelHashMap(NAME->data, [&](const auto & mapped, WriteBuffer & wb_) { state_serializer(mapped, wb_); }, wb); \
        else \
            DB::serializeHashMap(NAME->data, [&](const auto & mapped, WriteBuffer & wb_) { state_serializer(mapped, wb_); }, wb); \
    }

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
}

void MemoryAggregatedDataVariants::deserialize(ReadBuffer & rb, const IAggregator & aggregator_)
{
    chassert(aggregator_.type() == AggregatorType::Memory);
    const auto & memory_aggregator = static_cast<const MemoryAggregator &>(aggregator_);

    /// Serialization layout
    /// [version] + [states layout]
    VersionType recovered_version = 0;
    readIntBinary(recovered_version, rb);

    /// Check version compatibility
    static constexpr VersionType min_version = 3;
    if (recovered_version < min_version || recovered_version > version)
        throw Exception(
            ErrorCodes::BAD_VERSION,
            "Failed to recover aggregation function checkpoint, incompatible version: {}, expected version: {}",
            recovered_version,
            version);

    /// [states layout]
    bool inited = false;
    readBinary(inited, rb);
    if (!inited)
        return;

    UInt8 recovered_data_variants_type_uint8;
    readIntBinary<UInt8>(recovered_data_variants_type_uint8, rb);
    MemoryAggregatedDataVariants::Type recovered_data_variants_type
        = static_cast<MemoryAggregatedDataVariants::Type>(recovered_data_variants_type_uint8);

    aggregator = &memory_aggregator;
    memory_aggregator.initDataVariants(*this);
    /// Data variants is inited with single level hashmap, however the checkpoint states are 2 levels
    /// which means data variants was converted to two level
    if (type != recovered_data_variants_type)
        if (isConvertibleToTwoLevel())
            convertToTwoLevel();

    if (type != recovered_data_variants_type)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover aggregation checkpoint. Aggregated data variant type is not compatible, checkpointed={}, current={}",
            magic_enum::enum_name(recovered_data_variants_type),
            magic_enum::enum_name(type));

    UInt8 recovered_expanded_data_type_uint8;
    readIntBinary<UInt8>(recovered_expanded_data_type_uint8, rb);
    TrackingUpdatesType recovered_expanded_data_type = static_cast<TrackingUpdatesType>(recovered_expanded_data_type_uint8);
    if (recovered_expanded_data_type != memory_aggregator.params->tracking_updates_type)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover aggregation checkpoint. Expanded data type is not the same, checkpointed={}, current={}",
            magic_enum::enum_name(recovered_expanded_data_type),
            magic_enum::enum_name(memory_aggregator.params->tracking_updates_type));

    /// [aggregates_size] was added in V4
    if (recovered_version >= 4)
    {
        size_t recovered_aggregates_size = 0;
        readVarUInt(recovered_aggregates_size, rb);
        /// TODO: supports for adding aggregates
    }

    auto state_deserializer = [this, &memory_aggregator, recovered_version](auto & place, auto & rb_, Arena *) {
        place = nullptr; /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
        auto * aggregate_data
            = aggregates_pool->alignedAlloc(memory_aggregator.totalSizeOfAggregatedStates(), memory_aggregator.alignOfAggregatedStates());
        memory_aggregator.createAggregateStates(aggregate_data);
        place = aggregate_data;

        switch (memory_aggregator.params->tracking_updates_type)
        {
            case TrackingUpdatesType::UpdatesWithRetract:
            {
                TrackingUpdates::deserialize(place, rb_);

                auto & retract_place = TrackingUpdatesWithRetract::getRetract(place);
                bool has_retract = false;
                readBinary(has_retract, rb_);
                if (has_retract)
                {
                    auto * tmp_retract = retract_pool->alignedAlloc(
                        memory_aggregator.totalSizeOfAggregatedStates(), memory_aggregator.alignOfAggregatedStates());
                    memory_aggregator.createAggregateStates(tmp_retract);
                    retract_place = tmp_retract;

                    if (recovered_version >= 4)
                        TrackingUpdates::deserialize(retract_place, rb_);
                    else
                        /// Always mark as non empty for compatibility, use the middle value of uint64,
                        /// whether add() or negate(), ensure it is not empty
                        TrackingUpdates::data(retract_place).updates = std::numeric_limits<int64_t>::max();

                    memory_aggregator.deserializeAggregateStates(retract_place, rb_, retract_pool.get());
                }
                break;
            }
            case TrackingUpdatesType::Updates:
            {
                TrackingUpdates::deserialize(place, rb_);
                break;
            }
            default:
                break;
        }

        memory_aggregator.deserializeAggregateStates(place, rb_, aggregates_pool);
    };

    /// [aggr-func-state-without-key]
    if (type == MemoryAggregatedDataVariants::Type::without_key)
        state_deserializer(without_key, rb, aggregates_pool);

    /// [aggr-func-state-in-hash-map]
#define M(NAME, IS_TWO_LEVEL) \
    else if (type == MemoryAggregatedDataVariants::Type::NAME) \
    { \
        if constexpr (IS_TWO_LEVEL) \
            DB::deserializeTwoLevelHashMap( \
                NAME->data, \
                [&](auto & mapped, Arena & pool, ReadBuffer & rb_) { state_deserializer(mapped, rb_, &pool); }, \
                *aggregates_pool, \
                rb); \
        else \
            DB::deserializeHashMap( \
                NAME->data, \
                [&](auto & mapped, Arena & pool, ReadBuffer & rb_) { state_deserializer(mapped, rb_, &pool); }, \
                *aggregates_pool, \
                rb); \
    }

    APPLY_FOR_AGGREGATED_VARIANTS_STREAMING(M)
#undef M
    else throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
}

}

}
