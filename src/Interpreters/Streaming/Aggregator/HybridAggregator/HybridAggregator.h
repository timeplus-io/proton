#pragma once

#include <Interpreters/Streaming/Aggregator/AggregatingConvertParams.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregatedDataVariants.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregatorParams.h>
#include <Interpreters/Streaming/Aggregator/IAggregator.h>
#include <Common/serde.h>

/// HybridAggregator is an implementation from scratch to handle mutable data and large scale
/// key streaming aggregation scenarios.
namespace DB::Streaming
{

class HybridAggregator final : public IAggregator
{
public:
    explicit HybridAggregator(const Block & header_, const HybridAggregatorParamsPtr & hybrid_params_);

    std::string_view getName() const noexcept override { return "HybridAggregator"; }
    AggregatorType type() const noexcept override { return AggregatorType::Hybrid; }

    /// Should be called before execution
    void setSharedHybridHashTableConfig(std::string_view id, HybridHashTableConfig && config_)
    {
        shared_configs.emplace(id, std::move(config_));
    }
    const HybridHashTableConfig & getSharedHybridHashTableConfig(std::string_view id) const { return shared_configs.at(id); }

    /// Executing to aggregate states
    std::pair<bool, bool> executeOnBlock(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool new_keys) const override;

    std::pair<bool, bool> executeAndRetractOnBlock(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool new_keys) const override;

    Block executeAndFinalizePerRow(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool new_keys) const override;

    /// \return finalized block if there are `expired` key
    Block executeAndFinalizeAfterKeyExpire(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool new_keys) const override;

    /// Converting states to blocks
    BlocksList
    convertToBlocks(IAggregatedDataVariants & data_variants, size_t max_threads, AggregatingConvertParams & cparams) const override;
    BlocksList mergeAndConvertToBlocks(
        ManyIAggregatedDataVariants & many_data_variants, size_t max_threads, AggregatingConvertParams & cparams) const override;

    Block spliceAndConvertToBlock(IAggregatedDataVariants & variants, const std::vector<Int64> & gcd_buckets) const override;
    Block mergeAndSpliceAndConvertToBlock(
        ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const override;

    Block spliceAndConvertUpdatesToBlock(IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const override;
    Block mergeAndSpliceAndConvertUpdatesToBlock(
        ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const override;

    std::vector<Int64> buckets(const IAggregatedDataVariants & result) const override;
    void removeBucketsBefore(IAggregatedDataVariants & result, Int64 max_bucket, UInt64 transform_id) const override;
    void resetUpdatedForBuckets(IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const override;

private:
    HybridHashTableConfig getSubConfig(std::string_view id, std::string_view sub_name) const;

    std::pair<bool, bool> doExecuteOnBlock(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool new_keys,
        bool tracking_retracts) const;

    /// For case when there are no keys (all aggregate into one row). For UDA with own strategy,
    /// return 'true' means the UDA should emit after execution
    [[nodiscard]] bool executeWithoutKeyImpl(
        HybridAggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions) const;

    BlocksList convertToBlocksWithoutKey(HybridAggregatedDataVariants & data_variants, bool merged_variants, bool final_) const;
    BlocksList convertToBlocksWithoutKeyForRetractsMerged(HybridAggregatedDataVariants & data_variants, bool final_) const;
    BlocksList convertToBlocksWithoutKeyForRetracts(HybridAggregatedDataVariants & data_variants, bool final_) const;
    Block doConvertOnePlace(AggregateDataPtr data, const Block & res_header, bool final_) const;

    template <typename Table, typename KeyGetter>
    Block executeAndFinalizePerRowImpl(
        Table & table,
        const KeyGetter & key_getter,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        bool new_keys) const;

    Block executeAndFinalizeWithoutKeyPerRowImpl(
        AggregateDataPtr aggregate_data, size_t row_begin, size_t row_end, AggregateFunctionInstruction * aggregate_instructions) const;

    /// Process one data block, aggregate the data into a hash table.
    template <typename Table, typename KeyGetter>
    bool executeOnBlockImpl(
        Table & table,
        Table * updates,
        Table * retracts,
        const KeyGetter & key_getter,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        bool new_keys) const;

    /// Process one data block, aggregate the data into a hash table and return fianlized aggregated states for expired keys.
    template <typename Table, typename KeyList, typename KeyGetter>
    Block executeAndFinalizeAfterKeyExpireImpl(
        Table & table,
        KeyList & outstanding_keys,
        const IColumn * ts_col,
        const KeyGetter & key_getter,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        bool new_keys) const;

    template <typename KeyGetter, typename Table>
    BlocksList convertToBlocksMerged(Table & table, Table * retracts) const;

    template <typename KeyGetter, typename Table>
    BlocksList convertToBlocksImpl(Table & table, Table * updates, Table * retracts, bool clear_updates) const;

    template <typename KeyGetter, typename Table>
    BlocksList convertToBlocksForAll(Table & table) const;

    template <typename KeyGetter, typename Table>
    BlocksList convertToBlocksForUpdates(Table & table, Table * updates, bool clear_updates) const;

    template <typename KeyGetter, typename Table>
    BlocksList convertToBlocksForRetracts(Table & table, Table * retracts, Table * updates) const;

    template <typename KeyGetter, typename Table>
    BlocksList parallelConvertToBlocks(
        const std::vector<Table *> & src_tables, const std::vector<Table *> & src_updates, const std::vector<Table *> & src_retracts) const;

    template <typename KeyGetter, typename Table>
    Block convertOneBucketToBlock(Table & table, Table * updates) const;

    template <typename KeyGetter, typename Table>
    Block convertOneBucketToBlockMerged(Table & table) const;

    template <typename InnerTable, typename Table>
    void
    mergeGCDBuckets(InnerTable & dst, const std::map<Int64, InnerTable *> & gcd_bucket_tables, Table * src_updates, Arena & arena) const;

    template <typename InnerTable, typename ZeroOutBucketForKey>
    void doMergeGCDBuckets(
        InnerTable & dst,
        const std::map<Int64, InnerTable *> & gcd_bucket_tables,
        ZeroOutBucketForKey zero_out_bucket,
        Arena & arena) const;

    template <typename InnerTable, typename Table, typename ZeroOutBucketForKey>
    void doMergeGCDBucketsForUpdates(
        InnerTable & dst,
        const std::map<Int64, InnerTable *> & gcd_bucket_tables,
        Table * src_updates,
        ZeroOutBucketForKey zero_out_bucket,
        Arena & arena) const;

    Block doSpliceAndConvertToBlock(IAggregatedDataVariants & variants, const std::vector<Int64> & gcd_buckets) const;
    Block doMergeAndSpliceAndConvertToBlock(ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const;

private:
    void initStates(HybridAggregatedDataVariants & result) const;
    void calculateAggregateStateSizes();
    void createAggregateStates(AggregateDataPtr aggregate_data) const;
    void destroyAggregateStates(AggregateDataPtr aggregate_data) const;
    void serializeAggregateStates(ConstAggregateDataPtr place, WriteBuffer & wb) const;
    void deserializeAggregateStates(AggregateDataPtr place, ReadBuffer & rb, VersionType version = HybridAggregatedDataVariants::version) const;

    void saveAggregateStatesToRetract(AggregateDataPtr place) const;

private:
    void mergeWithoutKey(HybridAggregatedDataVariants & result, ManyIAggregatedDataVariants & many_data_variants, Arena & arena) const;

    template <typename KeyGetter, typename Table>
    void merge(
        Table & dst,
        Table * dst_retracts,
        const std::vector<Table *> & srcs,
        const std::vector<Table *> & src_updates,
        const std::vector<Table *> & src_retracts,
        Arena & arena) const;

    template <typename Table>
    void mergeNormal(Table & dst, const std::vector<Table *> & srcs, Arena & arena) const;

    template <typename KeyGetter, typename Table>
    void mergeUpdates(Table & dst, const std::vector<Table *> & srcs, const std::vector<Table *> & src_updates, Arena & arena) const;

    template <typename KeyGetter, typename Table>
    void mergeRetracts(
        Table & dst,
        Table * dst_retracts,
        const std::vector<Table *> & srcs,
        const std::vector<Table *> & src_updates,
        const std::vector<Table *> & src_retracts,
        Arena & arena) const;

private:
    friend struct HybridAggregatedDataVariants;

    HybridAggregatorParamsPtr hybrid_params;
    absl::flat_hash_map<String, HybridHashTableConfig> shared_configs;

    HybridHashType method_chosen = HybridHashType::Empty;
    mutable std::optional<size_t> bucket_key_offset;
    bool has_nullable_key = false;
};

}
