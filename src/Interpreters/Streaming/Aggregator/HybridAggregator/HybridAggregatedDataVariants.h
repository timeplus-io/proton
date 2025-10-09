#pragma once

#include <Interpreters/Streaming/Aggregator/IAggregatedDataVariants.h>
#include <Common/HybridHashTable/HybridHashTableTemplate.h>
#include <Common/HybridKeyList/HybridKeyListTemplate.h>
#include <Common/Rocks/RocksHandler.h>
#include <Common/serde.h>

namespace DB::Streaming
{

class HybridAggregator;

SERDE struct HybridAggregatedDataVariants final : public IAggregatedDataVariants
{
    explicit HybridAggregatedDataVariants(std::string id_)
        : id(std::move(id_)), without_key(nullptr, &std::free), without_key_retracts(nullptr, &std::free)
    {
    }
    ~HybridAggregatedDataVariants() override;

    std::string_view getID() const noexcept override { return id; }
    bool isLowCardinality() const noexcept override { return table.isLowCardinality(); }
    bool isTwoLevel() const noexcept override { return table.isTwoLevel(); }
    bool empty() const noexcept override { return table.empty(); }
    void updateMetrics(AggregatedDataMetrics & /*metrics*/, size_t /*total_size_of_aggregate_states*/) const override;
    AggregatorType aggregatorType() const noexcept override { return AggregatorType::Hybrid; }

    HybridHashType type() const noexcept { return table.getType(); }

    void serialize(WriteBuffer & wb, const IAggregator & aggregator_) const override;
    void deserialize(ReadBuffer & rb, const IAggregator & aggregator_) override;

    /// Write / read data to / from RocksDB
    void write(RocksHandlerPtr rocks_handler, const IAggregator & aggregator_);
    void read(RocksHandlerPtr rocks_handler, const IAggregator & aggregator_);

    void initWithoutKeyStates(size_t total_size_of_aggregate_states, size_t align_aggregate_states);
    void initWithoutKeyRetractStates(size_t total_size_of_aggregate_states, size_t align_aggregate_states);

    void reset() override;
    void resetRetractWithoutKey();

    SERDE std::string id;
    NO_SERDE const HybridAggregator * aggregator = nullptr;

    NO_SERDE std::vector<size_t> key_sizes; /// Dimensions of keys, if keys of fixed length

    /// There are 2 separate memory we will need to be taken care carefully
    /// 1) When aggregating without keys, we will use \without_key in the
    ///    data variant to hold the aggregated states.
    /// 2) When aggregating with keys, we will use \table in the data variant
    ///    to hold the aggregated states
    ///
    /// Aggregated states have 2 parts for each key
    /// 1) A vector of memory which holds (points to) each aggregating functions state objects, which is
    ///   allocated via std::malloc(total_size_of_aggregate_states)
    /// 2) The aggregating functions' state objects themselves, which are inited via
    ///   createAggregateStates(AggregateDataPtr)
    ///
    /// All states in \table will be constructed and destroyed automatically meaning
    /// it will call std::malloc(total_size_of_aggregate_states) and createAggregateStates(AggregateDataPtr)
    /// to init the states for new key; and call destroyAggregateStates(AggregateDataPtr) and std::free(...)
    /// automatically for each key / value maintained by it when it gets dtor-ed
    ///
    /// \without_key is different which requires us to maintain it manually : we will
    /// need call destroyAggregateStates(without_key.get()) when data variant gets dtored
    /// otherwise there is potential memory leak - states in aggregating function are leaked.
    ///
    /// Destroying aggregate states can happen in several scenarios btw
    /// 1) After converting states to blocks, we like to clear all of the states. This is
    ///    a very special case which requires clearing aggregated states : global aggregation
    ///    over global aggregation, the nested / inner global aggregation after converting
    ///    states to blocks is required to clear its states, otherwise the outer aggregation
    ///    result will not be correct. We will probably need fix this implementation by emitting
    ///    changelog for the inner global aggregation, then we don't need clearing the states for
    ///    all cases.
    /// 2) Temporary data variants which is used to hold merged states for multi-shards and after
    ///    converting the merged states, the temporary data variants will dtored and GC all its
    ///    merged states.
    /// 3) Regular data variants which hold the aggregated states, when they are dtored (when query
    ///    is killed / cancelled for example), they will need GC all the states.
    /// 4) Destroy / GC retract states and recreate empty retract states whenever there is a converting
    ///    of retract states to blocks and when data variants get dtored
    DataUniqPtr without_key;
    /// \without_key_retracts is used to tracking changes for without-key case
    DataUniqPtr without_key_retracts;
    NO_SERDE std::function<void(AggregateDataPtr)> without_key_states_constructor;
    NO_SERDE std::function<void(AggregateDataPtr)> without_key_states_destructor;

    HybridHashTableTemplate table;
    /// \changes is used to tracking changes since last emit
    HybridHashTableTemplate updates;
    /// \retracts is used to save last emits of each key
    HybridHashTableTemplate retracts;

    /// Keep tracking outstanding keys for `EMIT AFTER KEY EXPIRE`
    HybridKeyListTemplate outstanding_keys;

    /// V2 - outstanding keys is added
    /// V3 - 1. tracking state count for changelog inputs
    ///      2. add aggregates size
    static constexpr VersionType version = 3;
};

using HybridAggregatedDataVariantsPtr = std::shared_ptr<HybridAggregatedDataVariants>;
using ManyHybridAggregatedDataVariants = std::vector<HybridAggregatedDataVariantsPtr>;
using ManyHybridAggregatedDataVariantsPtr = std::shared_ptr<ManyHybridAggregatedDataVariants>;

}
