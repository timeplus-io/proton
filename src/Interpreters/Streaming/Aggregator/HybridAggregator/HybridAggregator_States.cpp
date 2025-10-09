#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingCount.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingTime.h>

namespace DB::Streaming
{
HybridHashTableConfig HybridAggregator::getSubConfig(std::string_view id, std::string_view sub_name) const
{
    chassert(!shared_configs.empty());
    auto it = shared_configs.find(id);
    if (it != shared_configs.end())
        return it->second.getSubConfig(sub_name);
    else
        return shared_configs.begin()->second.getSubConfig(sub_name, /*unshared=*/true);
}

void HybridAggregator::initStates(HybridAggregatedDataVariants & result) const
{
    if (method_chosen != HybridHashType::WithoutKey)
    {
        result.key_sizes = key_sizes;

        auto init_table = [&](HybridHashTableTemplate & table, std::string_view name) {
            auto config = getSubConfig(result.getID(), name);
            config.value_object_size = total_size_of_aggregate_states;
            config.align_value_object_size = align_aggregate_states;
            config.value_constructor = [this](void * data) { createAggregateStates(reinterpret_cast<AggregateDataPtr>(data)); };

            config.value_destructor = [this](void * data) { destroyAggregateStates(reinterpret_cast<AggregateDataPtr>(data)); };

            config.value_serializer = [this](const void * data, WriteBuffer & wb) {
                serializeAggregateStates(reinterpret_cast<ConstAggregateDataPtr>(data), wb);
                return ErrorCodes::OK;
            };

            config.value_deserializer = [this](void * data, ReadBuffer & rb) {
                deserializeAggregateStates(reinterpret_cast<AggregateDataPtr>(data), rb);
                return ErrorCodes::OK;
            };

            config.validate();
            table.init(method_chosen, std::move(config), result.key_sizes, logger, bucket_key_offset);
        };

        switch (params->tracking_updates_type)
        {
            case TrackingUpdatesType::UpdatesWithRetract:
            {
                init_table(result.retracts, "retracts");
                [[fallthrough]];
            }
            case TrackingUpdatesType::Updates:
            {
                auto config = getSubConfig(result.getID(), "changes");
                config.installNoOpCallbacks();
                config.validate();
                result.updates.init(method_chosen, std::move(config), result.key_sizes, logger, bucket_key_offset);
                [[fallthrough]];
            }
            case TrackingUpdatesType::None:
            {
                init_table(result.table, "main");
                break;
            }
        }

        if (params->emit_key_params)
        {
            auto table_config = getSubConfig(result.getID(), "list");

            HybridKeyListConfig list_config;
            list_config.spill_dir_path.swap(table_config.spill_dir_path);
            list_config.db_options.swap(table_config.db_options);
            list_config.ttl = table_config.ttl;
            list_config.use_hash_index = table_config.use_hash_index;
            list_config.max_hot_key_count = table_config.max_hot_key_count;
            list_config.cleanup_on_disk_data = table_config.cleanup_on_disk_data;
            list_config.handle_id.swap(table_config.handle_id);
            list_config.rocks_handler_getter.swap(table_config.rocks_handler_getter);

            list_config.validate();

            result.outstanding_keys.init(method_chosen, std::move(list_config), result.key_sizes, logger);
        }
    }
    else
    {
        result.without_key_states_constructor = [this](AggregateDataPtr data) { createAggregateStates(data); };
        result.without_key_states_destructor = [this](AggregateDataPtr data) { destroyAggregateStates(data); };
        result.initWithoutKeyStates(total_size_of_aggregate_states, align_aggregate_states);
    }
}

void HybridAggregator::createAggregateStates(AggregateDataPtr aggregate_data) const
{
    if (trackingStateCount())
        TrackingCount::init(aggregate_data + tracking_count_offset);

    if (trackingStateTime())
        TrackingTime::init(aggregate_data + tracking_time_offset);

    for (size_t j = 0; j < params->aggregates_size; ++j)
    {
        try
        {
            /// An exception may occur if there is a shortage of memory.
            /// In order that then everything is properly destroyed, we "roll back" some of the created states.
            /// The code is not very convenient.
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

void HybridAggregator::destroyAggregateStates(AggregateDataPtr aggregate_data) const
{
    doDestroyAggregateStates(aggregate_data);
}

void HybridAggregator::serializeAggregateStates(ConstAggregateDataPtr place, DB::WriteBuffer & wb) const
{
    chassert(place);

    if (trackingStateCount()) /// Added in V3
        TrackingCount::serialize(place + tracking_count_offset, wb);

    if (trackingStateTime())
        TrackingTime::serialize(place + tracking_time_offset, wb);

    for (size_t i = 0; i < params->aggregates_size; ++i)
        aggregate_functions[i]->serialize(place + offsets_of_aggregate_states[i], wb);
}

void HybridAggregator::deserializeAggregateStates(AggregateDataPtr place, ReadBuffer & rb, VersionType version) const
{
    chassert(place);

    if (trackingStateCount())
    {
        if (version >= 3)
            TrackingCount::deserialize(place + tracking_count_offset, rb);
        else
            /// Always mark as non empty for compatibility, use the middle value of uint64,
            /// whether add() or negate(), ensure it is not empty
            TrackingCount::data(place + tracking_count_offset).count = std::numeric_limits<int64_t>::max();
    }

    if (trackingStateTime())
        TrackingTime::deserialize(place + tracking_time_offset, rb);

    for (size_t i = 0; i < params->aggregates_size; ++i)
        aggregate_functions[i]->deserialize(place + offsets_of_aggregate_states[i], rb, std::nullopt, /*arena=*/nullptr); /// FIXME, arena
}
}
