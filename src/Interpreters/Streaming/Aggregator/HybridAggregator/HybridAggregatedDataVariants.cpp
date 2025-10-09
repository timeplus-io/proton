#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregatedDataVariants.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/IAggregator.h>

#include <Common/MemoryHelpers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int RECOVER_CHECKPOINT_FAILED;
extern const int BAD_VERSION;
}

namespace Streaming
{

HybridAggregatedDataVariants::~HybridAggregatedDataVariants()
{
    reset();
}

void HybridAggregatedDataVariants::reset()
{
    if (without_key)
    {
        without_key_states_destructor(without_key.get());
        without_key.reset();
    }

    resetRetractWithoutKey();

    table.reset();
    updates.reset();
    retracts.reset();
}

void HybridAggregatedDataVariants::resetRetractWithoutKey()
{
    if (without_key_retracts)
        without_key_states_destructor(without_key_retracts.get());

    without_key_retracts.reset();
}

void HybridAggregatedDataVariants::initWithoutKeyStates(size_t total_size_of_aggregate_states, size_t align_aggregate_states)
{
    chassert(without_key_states_constructor);

    /// Set the hash type to make table inited
    table.setWithoutKeyType();

    auto place = alignedAllocate(total_size_of_aggregate_states, align_aggregate_states);
    without_key_states_constructor(place.get());
    without_key.swap(place);
}

void HybridAggregatedDataVariants::initWithoutKeyRetractStates(size_t total_size_of_aggregate_states, size_t align_aggregate_states)
{
    chassert(without_key_states_constructor);

    auto retract_place = alignedAllocate(total_size_of_aggregate_states, align_aggregate_states);
    without_key_states_constructor(retract_place.get());
    without_key_retracts.swap(retract_place);
}

void HybridAggregatedDataVariants::updateMetrics(AggregatedDataMetrics & metrics, size_t total_size_of_aggregate_states) const
{
    if (table.isWithoutKeyType())
    {
        metrics.total_aggregated_rows += 1;
        metrics.total_bytes_of_aggregate_states += total_size_of_aggregate_states;
    }
    else
    {
        metrics.total_aggregated_rows += table.approximateCount();
        metrics.hash_buffer_bytes += table.getBufferSizeInBytes();
        metrics.hash_buffer_cells += table.getBufferSizeInCells();
        metrics.total_bytes_of_aggregate_states += table.approximateCount() * total_size_of_aggregate_states;
        metrics.additional_metrics.emplace_back(table.metrics().string());

        if (!updates.empty())
            metrics.additional_metrics.emplace_back(
                fmt::format("updates rows={} ({})", updates.approximateCount(), updates.metrics().string()));

        if (!retracts.empty())
            metrics.additional_metrics.emplace_back(
                fmt::format("retracts rows={} ({})", retracts.approximateCount(), retracts.metrics().string()));
    }
}

void HybridAggregatedDataVariants::serialize(WriteBuffer & wb, const IAggregator & aggregator_) const
{
    chassert(aggregator_.type() == AggregatorType::Hybrid);
    const auto & hybrid_aggregator = static_cast<const HybridAggregator &>(aggregator_);

    /// Layout: version, id, inited[, aggregates_size(V3), is_without_key_type ...]
    ///  - (is_without_key_type = true): ..., without_key, has_without_key_retracts[, without_key_retracts]
    ///  - (is_without_key_type = false): ..., table[, updates][, retracts][, outstanding_keys(V2)]
    writeBinary(version, wb);

    writeStringBinary(id, wb);

    bool inited = !empty();
    writeBinary(inited, wb);
    if (!inited)
        return;

    writeVarUInt(hybrid_aggregator.getParams()->aggregates_size, wb); /// Added in V3

    bool is_without_key_type = table.isWithoutKeyType();
    writeBinary(is_without_key_type, wb);
    if (is_without_key_type)
    {
        chassert(without_key);
        hybrid_aggregator.serializeAggregateStates(without_key.get(), wb);

        bool has_without_key_retracts = without_key_retracts != nullptr;
        writeBinary(has_without_key_retracts, wb);
        if (has_without_key_retracts)
            hybrid_aggregator.serializeAggregateStates(without_key_retracts.get(), wb);
    }
    else
    {
        table.serialize(wb);

        bool has_updates = !updates.empty();
        writeBinary(has_updates, wb);
        if (has_updates)
            updates.serialize(wb);

        bool has_retracts = !retracts.empty();
        writeBinary(has_retracts, wb);
        if (has_retracts)
            retracts.serialize(wb);

        bool has_outstanding_keys = !outstanding_keys.empty();
        writeBinary(has_outstanding_keys, wb);
        if (has_outstanding_keys)
            outstanding_keys.serialize(wb);
    }
}

void HybridAggregatedDataVariants::deserialize(ReadBuffer & rb, const IAggregator & aggregator_)
{
    chassert(aggregator_.type() == AggregatorType::Hybrid);
    const auto & hybrid_aggregator = static_cast<const HybridAggregator &>(aggregator_);

    VersionType recovered_version = 0;
    readBinary(recovered_version, rb);

    /// Check version compatibility
    if (recovered_version > version)
        throw Exception(
            ErrorCodes::BAD_VERSION,
            "Failed to recover hybrid aggregated state, incompatible version: {}, expected version: {}",
            recovered_version,
            version);

    readStringBinary(id, rb);

    bool inited = false;
    readBinary(inited, rb);
    if (!inited)
        return;

    chassert(empty());
    hybrid_aggregator.initStates(*this);

    /// [aggregates_size] was added in state V3
    if (recovered_version >= 3)
    {
        size_t recovered_aggregates_size = 0;
        readVarUInt(recovered_aggregates_size, rb);
        /// TODO: supports for adding aggregates
    }

    bool is_without_key_type = false;
    readBinary(is_without_key_type, rb);
    if (is_without_key_type)
    {
        if (!without_key)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid aggregated state: expected {} state, but got without key state",
                magic_enum::enum_name(type()));

        hybrid_aggregator.deserializeAggregateStates(without_key.get(), rb, recovered_version);

        bool has_without_key_retracts = false;
        readBinary(has_without_key_retracts, rb);
        if (has_without_key_retracts)
        {
            if (!without_key_retracts)
                initWithoutKeyRetractStates(hybrid_aggregator.totalSizeOfAggregatedStates(), hybrid_aggregator.alignOfAggregatedStates());

            hybrid_aggregator.deserializeAggregateStates(without_key_retracts.get(), rb, recovered_version);
        }
    }
    else
    {
        std::function<int(void *, ReadBuffer &)> old_value_deserializer;
        if (recovered_version <= 2)
            old_value_deserializer = [&hybrid_aggregator, recovered_version](void * data, ReadBuffer & rb_) {
                hybrid_aggregator.deserializeAggregateStates(reinterpret_cast<AggregateDataPtr>(data), rb_, recovered_version);
                return ErrorCodes::OK;
            };

        table.deserialize(rb, old_value_deserializer);

        bool has_updates = false;
        readBinary(has_updates, rb);
        if (has_updates)
        {
            if (updates.empty())
                throw Exception(
                    ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                    "Failed to recover hybrid aggregated state: expected none of updates state, but got updates state");

            updates.deserialize(rb);
        }

        bool has_retracts = false;
        readBinary(has_retracts, rb);
        if (has_retracts)
        {
            if (retracts.empty())
                throw Exception(
                    ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                    "Failed to recover hybrid aggregated state: expected none of retracts state, but got retracts state");

            retracts.deserialize(rb, old_value_deserializer);
        }

        if (recovered_version >= 2)
        {
            bool has_outstanding_keys = false;
            readBinary(has_outstanding_keys, rb);
            if (has_outstanding_keys)
            {
                if (outstanding_keys.empty())
                    throw Exception(
                        ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                        "Failed to recover hybrid aggregated state: expected none of outstanding keys, but got outstanding keys");

                outstanding_keys.deserialize(rb);
            }
        }
    }
}

void HybridAggregatedDataVariants::write(RocksHandlerPtr handler, const IAggregator & aggregator_)
{
    chassert(aggregator_.type() == AggregatorType::Hybrid);
    const auto & hybrid_aggregator = static_cast<const HybridAggregator &>(aggregator_);

    /// Layout: version, id, inited[, aggregates_size(V3), ...]
    ///  - (is_without_key_type = true): ..., without_key[, without_key_retracts]
    ///  - (is_without_key_type = false): ..., table[, updates][, retracts]
    handler->put("version", version);

    handler->put("id", id);

    bool inited = !empty();
    handler->put("inited", inited);
    if (inited)
    {
        handler->put("aggregates_size", hybrid_aggregator.getParams()->aggregates_size); /// Added in V3

        if (table.isWithoutKeyType())
        {
            chassert(without_key);
            WriteBufferFromOwnString wb;
            hybrid_aggregator.serializeAggregateStates(without_key.get(), wb);
            handler->put("without_key", wb.str());

            if (without_key_retracts)
            {
                WriteBufferFromOwnString wb2;
                hybrid_aggregator.serializeAggregateStates(without_key_retracts.get(), wb2);
                handler->put("without_key_retracts", wb2.str());
            }
        }
        else
        {
            table.flush();
            updates.flush();
            retracts.flush();
            outstanding_keys.flush();

            if (isTwoLevel())
            {
                chassert(!table.empty());
                handler->put("table_buckets", table.buckets());

                if (!updates.empty())
                    handler->put("updates_buckets", updates.buckets());

                if (!retracts.empty())
                    handler->put("retracts_buckets", retracts.buckets());
            }
        }
    }
}

void HybridAggregatedDataVariants::read(RocksHandlerPtr handler, const IAggregator & aggregator_)
{
    chassert(aggregator_.type() == AggregatorType::Hybrid);
    const auto & hybrid_aggregator = static_cast<const HybridAggregator &>(aggregator_);

    VersionType recovered_version = 0;
    handler->get("version", recovered_version);

    /// Check version compatibility
    if (recovered_version > version)
        throw Exception(
            ErrorCodes::BAD_VERSION,
            "Failed to recover hybrid aggregated state, incompatible version: {}, expected version: {}",
            recovered_version,
            version);

    handler->get("id", id);

    bool inited = false;
    handler->get("inited", inited);
    if (!inited)
        return;

    chassert(empty());
    hybrid_aggregator.initStates(*this);

    if (recovered_version >= 3)
    {
        size_t recovered_aggregates_size = 0;
        handler->get("aggregates_size", recovered_aggregates_size);
        /// TODO: supports for adding aggregates
    }

    std::string_view without_key_state;
    if (handler->tryGet("without_key", without_key_state))
    {
        if (!without_key) [[unlikely]]
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid aggregated state: expected {} state, but got without key state",
                type());

        ReadBufferFromString rb(without_key_state);
        hybrid_aggregator.deserializeAggregateStates(without_key.get(), rb, recovered_version);

        std::string_view without_key_retracts_state;
        if (handler->tryGet("without_key_retracts", without_key_retracts_state))
        {
            if (!without_key_retracts)
                initWithoutKeyRetractStates(hybrid_aggregator.totalSizeOfAggregatedStates(), hybrid_aggregator.alignOfAggregatedStates());

            ReadBufferFromString rb2(without_key_retracts_state);
            hybrid_aggregator.deserializeAggregateStates(without_key_retracts.get(), rb2, recovered_version);
        }
    }
    else
    {
        std::vector<Int64> buckets;
        if (handler->tryGet("table_buckets", buckets))
        {
            chassert(!table.empty());
            table.reinitBuckets(buckets);

            if (handler->tryGet("updates_buckets", buckets))
            {
                if (updates.empty()) [[unlikely]]
                    throw Exception(
                        ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                        "Failed to recover hybrid aggregated state: expected none of updates state, but got updates state");

                updates.reinitBuckets(buckets);
            }

            if (handler->tryGet("retracts_buckets", buckets))
            {
                if (retracts.empty()) [[unlikely]]
                    throw Exception(
                        ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                        "Failed to recover hybrid aggregated state: expected none of retracts state, but got retracts state");

                retracts.reinitBuckets(buckets);
            }
        }

        std::function<int(void *, ReadBuffer &)> old_value_deserializer;
        if (recovered_version <= 2)
            old_value_deserializer = [&hybrid_aggregator, recovered_version](void * data, ReadBuffer & rb) {
                hybrid_aggregator.deserializeAggregateStates(reinterpret_cast<AggregateDataPtr>(data), rb, recovered_version);
                return ErrorCodes::OK;
            };

        table.reload(old_value_deserializer);
        updates.reload();
        retracts.reload(old_value_deserializer);
        outstanding_keys.reload();
    }
}
}

}
