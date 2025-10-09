#pragma once

#include <Interpreters/Streaming/Aggregator/AggregateFunctionInstruction.h>
#include <Interpreters/Streaming/Aggregator/AggregatingConvertParams.h>
#include <Interpreters/Streaming/Aggregator/IAggregatedDataVariants.h>
#include <Interpreters/Streaming/Aggregator/IAggregatorParams.h>

namespace DB
{
using AggregatedDataWithoutKey = AggregateDataPtr;

class CompiledAggregateFunctionsHolder;

namespace Streaming
{

class IAggregator
{
public:
    IAggregator(IAggregatorParamsPtr params_, const Block & input_header_, const std::string & aggregator);

    virtual ~IAggregator() = default;
    virtual AggregatorType type() const noexcept = 0;
    virtual std::string_view getName() const noexcept = 0;
    IAggregatorParamsPtr getParams() const noexcept { return params; }
    Block getHeader(bool final_) const { return params->getHeader(input_header, final_); }
    size_t totalSizeOfAggregatedStates() const noexcept { return total_size_of_aggregate_states; }
    size_t alignOfAggregatedStates() const noexcept { return align_aggregate_states; }

    /// Process one block. Return {should_abort, need_finalization} pair
    /// should_abort: if the processing should be aborted (with group_by_overflow_mode = 'break') return true, otherwise false.
    /// need_finalization : only for UDA aggregation. If there is no UDA, always false
    /// \param new_keys if the new inserted keys are new, HybridAggregator can leverage this as an optimization
    virtual std::pair<bool, bool> executeOnBlock(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool new_keys) const
        = 0;

    /// For EMIT CHANGELOG
    /// Execute and retract state for changed groups:
    /// 1) For new group:
    ///     \retracted_result: add an elem <group_key, null> if not exists
    ///     \result:           add an elem <group_key, current_state>
    /// 2) For updated group:
    ///     \retracted_result: add an elem <group_key, last_state> if not exists
    ///     \result:           update the elem <group_key, current_state>
    /// 3) For deleted group:
    ///     \retracted_result: add an elem <group_key, last_state> if not exists
    ///     \result:           delete the <group_key> group
    /// \returns {should_abort, need_finalization} bool pair
    /// should_abort: if the processing should be aborted (with group_by_overflow_mode = 'break') return true, otherwise false.
    /// need_finalization : only for UDA aggregation. If there is no UDA, always false
    /// \param new_keys if the new inserted keys are new, HybridAggregator can leverage this as an optimization
    virtual std::pair<bool, bool> executeAndRetractOnBlock(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool new_keys) const
        = 0;

    /// executeAndFinalizePerRow aggregates columns and then finalize per row, The finalized result will be returned.
    /// Used by `EMIT PER EVENT`
    virtual Block executeAndFinalizePerRow(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool new_keys) const
        = 0;

    /// executeAndFinalizeAfterKeyExpire aggregates columns and then finalize the expired keys. The finalized result will be returned.
    /// After finalization, the expired keys will be removed from the aggregating hash table.
    /// Used by `EMIT AFTER KEY EXPIRE`
    virtual Block executeAndFinalizeAfterKeyExpire(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        bool new_keys) const
        = 0;

    ///  \param max_threads      - limits max threads for converting two level aggregate state in parallel
    virtual BlocksList
    convertToBlocks(IAggregatedDataVariants & data_variants, size_t max_threads, AggregatingConvertParams & cparams) const
        = 0;
    virtual BlocksList
    mergeAndConvertToBlocks(ManyIAggregatedDataVariants & many_data_variants, size_t max_threads, AggregatingConvertParams & cparams) const
        = 0;

    /// Window aggregation
    /// For Tumble/Session window function, there is only one bucket
    /// For Hop window function, merge multiple gcd windows (buckets) to a hop window
    /// For examples:
    ///   gcd_bucket1 - [00:00, 00:02)
    ///                            =>  result block - [00:00, 00:04)
    ///   gcd_bucket2 - [00:02, 00:04)
    virtual Block spliceAndConvertToBlock(IAggregatedDataVariants & variants, const std::vector<Int64> & gcd_buckets) const = 0;
    virtual Block
    mergeAndSpliceAndConvertToBlock(ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const
        = 0;

    /// Similar to 'spliceAndConvertToBlock', but only convert the states of update groups tracked
    /// NOTE: Specially, we cannot reset the updated flag during the conversion process, because each window has overlapping gcd buckets
    /// and needs to be manually reset by calling `resetUpdatedOfBuckets` after all hop windows conversions are completed.
    virtual Block spliceAndConvertUpdatesToBlock(IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const = 0;
    virtual Block
    mergeAndSpliceAndConvertUpdatesToBlock(ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const
        = 0;

    virtual std::vector<Int64> buckets(const IAggregatedDataVariants & result) const = 0;
    virtual void removeBucketsBefore(IAggregatedDataVariants & result, Int64 max_bucket, UInt64 transform_id) const = 0;
    virtual void resetUpdatedForBuckets(IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const = 0;

    Columns materializeKeyColumns(const Columns & columns, ColumnRawPtrs & key_columns, bool is_low_cardinality) const;

protected:
    /// For some streaming queries with `emit on update` or `emit changelog`, need tracking updates (with retract)
    bool needTrackUpdates() const noexcept { return params->tracking_updates_type != TrackingUpdatesType::None; }

    bool trackingStateCount() const noexcept { return params->delta_col_pos >= 0 || params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract; }
    bool trackingStateTime() const noexcept { return params->emit_key_params.has_value(); }

    void prepareAggregateInstructions(
        const Columns & columns,
        std::vector<ColumnRawPtrs> & aggregate_columns,
        Columns & materialized_columns,
        std::vector<AggregateFunctionInstruction> & aggregate_functions_instructions,
        NestedColumnsHolder & nested_columns_holder) const;

    struct OutputBlockColumns
    {
        MutableColumns key_columns;
        std::vector<IColumn *> raw_key_columns;
        MutableColumns aggregate_columns;
        MutableColumns final_aggregate_columns;
        AggregateColumnsData aggregate_columns_data;
    };

    OutputBlockColumns prepareOutputBlockColumns(const Block & res_header, const Arenas & aggregates_pools, bool final, size_t rows) const;

    Block finalizeBlock(const Block & res_header, OutputBlockColumns && out_cols, bool final, size_t rows) const;

    void insertAggregatesIntoColumns(AggregateDataPtr mapped, const MutableColumns & final_aggregate_columns, Arena * arena) const;

    void addAndInsertAggregatesIntoColumns(
        AggregateDataPtr aggregate_data,
        AggregateFunctionInstruction * aggregate_instructions,
        const MutableColumns & final_aggregate_columns,
        size_t row_num,
        Arena * arena) const;

    Block insertResultsIntoColumns(
        PaddedPODArray<AggregateDataPtr> & places, OutputBlockColumns && out_cols, Arena * arena, bool use_compiled_functions) const;
    Block insertResultsIntoColumns(PaddedPODArray<ConstAggregateDataPtr> & places, OutputBlockColumns && out_cols, Arena * arena) const;

    void mergeAggregateStates(
        AggregateDataPtr dst, ConstAggregateDataPtr src, Arena * arena, bool skip_compiled_aggregate_functions = false) const;

    void doDestroyAggregateStates(AggregateDataPtr place) const;

private:
    ColumnNumbers calculateKeysPositions();

protected:
    static bool hasSparseArguments(AggregateFunctionInstruction * aggregate_instructions);

    IAggregatorParamsPtr params;

    /// Data structure of source blocks.
    Block input_header;

    std::vector<size_t> key_sizes;
    AggregateFunctionsPlainPtrs aggregate_functions;

    /// Positions of aggregation key columns in the header.
    ColumnNumbers keys_positions;

    /// The offsets of tracking additional aggregate state
    size_t tracking_count_offset = 0;
    size_t tracking_time_offset = 0;

    std::vector<size_t> offsets_of_aggregate_states; /// The offset to the n-th aggregate function in a row of aggregate functions.
    size_t total_size_of_aggregate_states = 0; /// The total size of the row from the aggregate functions.

    // Add info to track alignment requirement
    // If there are states whose alignment are v1, ..., vn, align_aggregate_states will be max(v1, ... vn)
    size_t align_aggregate_states = 1;

    bool all_aggregates_has_trivial_destructor = false;

    /// Metrics
    /// How many RAM were used to process the query before processing the first block.
    Int64 memory_usage_before_aggregation = 0;

#if USE_EMBEDDED_COMPILER
    std::shared_ptr<CompiledAggregateFunctionsHolder> compiled_aggregate_functions_holder;
#endif

    std::vector<bool> is_aggregate_function_compiled;

    /// Try to compile aggregate functions.
    void compileAggregateFunctionsIfNeeded();

    LoggerPtr logger;
};

using IAggregatorPtr = std::shared_ptr<IAggregator>;

}

}
