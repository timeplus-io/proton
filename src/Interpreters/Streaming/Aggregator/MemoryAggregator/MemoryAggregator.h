#pragma once

#include <Interpreters/AggregationCommon.h>
#include <Interpreters/CompiledAggregateFunctionsHolder.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/Arena.h>
#include <Common/ThreadPool.h>
#include <Common/filesystemHelpers.h>

#include <Checkpoint/CheckpointContext.h>
#include <Interpreters/Streaming/Aggregator/AggregatingConvertParams.h>
#include <Interpreters/Streaming/Aggregator/IAggregator.h>
#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregatedDataVariants.h>
#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregatorParams.h>

namespace DB
{
namespace Streaming
{
/** Different data structures that can be used for aggregation
  * For efficiency, the aggregation data itself is put into the pool.
  * Data and pool ownership (states of aggregate functions)
  *  is acquired later - in `convertToBlocks` function, by the ColumnAggregateFunction object.
  *
  * Most data structures exist in two versions: normal and two-level (TwoLevel).
  * A two-level hash table works a little slower with a small number of different keys,
  *  but with a large number of different keys scales better, because it allows
  *  parallelize some operations (merging, post-processing) in a natural way.
  *
  * To ensure efficient work over a wide range of conditions,
  *  first single-level hash tables are used,
  *  and when the number of different keys is large enough,
  *  they are converted to two-level ones.
  *
  * PS. There are many different approaches to the effective implementation of parallel and distributed aggregation,
  *  best suited for different cases, and this approach is just one of them, chosen for a combination of reasons.
  */

/** How are "total" values calculated with WITH TOTALS?
  * (For more details, see TotalsHavingTransform.)
  *
  * In the absence of group_by_overflow_mode = 'any', the data is aggregated as usual, but the states of the aggregate functions are not finalized.
  * Later, the aggregate function states for all rows (passed through HAVING) are merged into one - this will be TOTALS.
  *
  * If there is group_by_overflow_mode = 'any', the data is aggregated as usual, except for the keys that did not fit in max_rows_to_group_by.
  * For these keys, the data is aggregated into one additional row - see below under the names `overflow_row`, `overflows`...
  * Later, the aggregate function states for all rows (passed through HAVING) are merged into one,
  *  also overflow_row is added or not added (depending on the totals_mode setting) also - this will be TOTALS.
  */

/** Aggregates the source of the blocks.
  */
class MemoryAggregator final : public IAggregator
{
public:
    MemoryAggregator(const Block & header_, const MemoryAggregatorParamsPtr & memory_params_);

    AggregatorType type() const noexcept override { return AggregatorType::Memory; }
    std::string_view getName() const noexcept override { return "MemoryAggregator"; }

    /// Process one block. Return {should_abort, need_finalization} pair
    /// should_abort: if the processing should be aborted (with group_by_overflow_mode = 'break') return true, otherwise false.
    /// need_finalization : only for UDA aggregation. If there is no UDA, always false
    std::pair<bool, bool> executeOnBlock(
        const Block & block,
        MemoryAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns /// Passed to not create them anew for each block
    ) const;

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
        AggregateColumns & aggregate_columns) const override;

    Block executeAndFinalizeAfterSessionClose(
        Columns columns,
        size_t row_begin,
        size_t row_end,
        IAggregatedDataVariants & result,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns) const override;

    /** Convert the aggregation data structure into a block.
      * If final = false, then ColumnAggregateFunction is created as the aggregation columns with the state of the calculations,
      *  which can then be combined with other states (for distributed query processing or checkpoint).
      * If final = true, then columns with ready values are created as aggregate columns.
      *
      * \param max_threads      - limits max threads for converting two level aggregate state in parallel
      */
    BlocksList
    convertToBlocks(IAggregatedDataVariants & data_variants, size_t max_threads, AggregatingConvertParams & cparams) const override;
    BlocksList mergeAndConvertToBlocks(
        ManyIAggregatedDataVariants & many_data_variants, size_t max_threads, AggregatingConvertParams & cparams) const override;

    /// For Tumble/Session window function, there is only one bucket
    /// For Hop window function, merge multiple gcd windows (buckets) to a hop window
    /// For examples:
    ///   gcd_bucket1 - [00:00, 00:02)
    ///                            =>  result block - [00:00, 00:04)
    ///   gcd_bucket2 - [00:02, 00:04)
    Block spliceAndConvertToBlock(IAggregatedDataVariants & variants, const std::vector<Int64> & gcd_buckets) const override;
    Block mergeAndSpliceAndConvertToBlock(
        ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const override;

    /// Similar to 'spliceAndConvertToBlock', but only convert the states of update groups tracked
    /// NOTE: Specially, we cannot reset the updated flag during the conversion process, because each window has overlapping gcd buckets
    /// and needs to be manually reset by calling `resetUpdatedOfBuckets` after all hop windows conversions are completed.
    Block spliceAndConvertUpdatesToBlock(IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const override;
    Block mergeAndSpliceAndConvertUpdatesToBlock(
        ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const override;
    void resetUpdatedForBuckets(IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const override;

    std::vector<Int64> bucketsBefore(const MemoryAggregatedDataVariants & result, Int64 max_bucket) const;
    std::vector<Int64> buckets(const IAggregatedDataVariants & result) const override;
    void removeBucketsBefore(IAggregatedDataVariants & result, Int64 max_bucket, UInt64 transform_id) const override;

private:
    [[nodiscard]] bool executeWithoutKeyImpl(
        AggregatedDataWithoutKey res,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        const ArenaPtr & pool) const;

    template <typename Method>
    Block executeAndFinalizePerRowImpl(
        Method & method,
        MemoryAggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions) const;

    Block executeAndFinalizeWithoutKeyPerRowImpl(
        AggregateDataPtr aggregate_data,
        size_t row_begin,
        size_t row_end,
        AggregateFunctionInstruction * aggregate_instructions,
        const ArenaPtr & pool) const;

    Block prepareBlockAndFillWithoutKey(MemoryAggregatedDataVariants & data_variants, AggregatingConvertParams & cparams) const;
    Block prepareBlockAndFillWithoutKeyNormal(MemoryAggregatedDataVariants & data_variants, AggregatingConvertParams & cparams) const;
    template <typename TrackingUpdatesStruct>
    Block prepareBlockAndFillWithoutKeyForUpdates(MemoryAggregatedDataVariants & data_variants, AggregatingConvertParams & cparams) const;
    Block prepareBlockAndFillWithoutKeyForRetract(MemoryAggregatedDataVariants & data_variants, AggregatingConvertParams & cparams) const;

    /// If \param always_merge_into_empty is true, always add an empty variants at front even if there is only one
    ManyMemoryAggregatedDataVariantsPtr
    prepareVariantsToMerge(ManyIAggregatedDataVariants & many_data_variants, bool always_merge_into_empty = false) const;

    /// \return: merged data, when there is no data, return nullptr
    IAggregatedDataVariantsPtr mergeGroups(ManyIAggregatedDataVariants & many_data_variants, AggregatingConvertParams & cparams) const;

    BlocksList doConvertToBlocks(IAggregatedDataVariants & variants, size_t max_threads, AggregatingConvertParams & cparams) const;

    BlocksList mergeAndConvertTwoLevelToBlocks(ManyMemoryAggregatedDataVariantsPtr prepared_data_ptr, size_t max_threads) const;

    void initStatesForWithoutKey(MemoryAggregatedDataVariants & data_variants) const;

    /// Select the aggregation method based on the number and types of keys.
    MemoryAggregatedDataVariants::Type chooseAggregationMethod();

    std::optional<MemoryAggregatedDataVariants::Type> chooseAggregationMethodTimeBucketTwoLevel(
        const DataTypes & types_removed_nullable,
        bool has_nullable_key,
        bool has_low_cardinality,
        size_t num_fixed_contiguous_keys,
        size_t keys_bytes) const;

    /** Create states of aggregate functions for one key.
      */
    void createAggregateStates(AggregateDataPtr & aggregate_data, bool skip_compiled_aggregate_functions = false) const;

    /** Call `destroy` methods for states of aggregate functions.
      * Used in the exception handler for aggregation, since RAII in this case is not applicable.
      */
    void destroyAllAggregateStates(MemoryAggregatedDataVariants & result) const;

    bool executeImpl(
        MemoryAggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions) const;

    /// Process one data block, aggregate the data into a hash table.
    template <typename Method>
    bool executeImplBatch(
        Method & method,
        MemoryAggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions) const;

    template <typename Method>
    void writeToTemporaryFileImpl(MemoryAggregatedDataVariants & data_variants, Method & method, TemporaryFileStream & out) const;

    /// Merge data from hash table `src` into `dst`.
    using EmptyKeyHandler = void *;
    void mergeWithoutKeyDataImpl(ManyMemoryAggregatedDataVariants & non_empty_data) const;

    template <typename Method>
    void mergeSingleLevelDataImpl(ManyMemoryAggregatedDataVariants & non_empty_data) const;

    template <bool return_single_block>
    using ConvertToBlockRes = std::conditional_t<return_single_block, Block, BlocksList>;

    template <bool return_single_block, typename Method, typename Table>
    ConvertToBlockRes<return_single_block>
    convertToBlockImpl(Method & method, Table & data, const ArenaPtr & pool, size_t rows, AggregatingConvertParams & cparams) const;

    template <typename Method, typename Table, typename ConvertCallback>
    void convertState(Table & data, AggregatingConvertParams & cparams, ConvertCallback && callback) const;

    template <bool return_single_block, typename Method, typename Table>
    ConvertToBlockRes<return_single_block> convertToBlockImplFinal(
        Method & method, Table & data, const ArenaPtr & pool, size_t rows, AggregatingConvertParams & cparams, bool use_compiled_functions)
        const;

    template <typename Method>
    Block convertOneBucketToBlockImpl(Method & method, const ArenaPtr & pool, Int64 bucket, AggregatingConvertParams & cparams) const;

    auto getZeroOutWindowKeysFunc(const ArenaPtr & pool) const;

    template <typename Method>
    BlocksList mergeAndConvertTwoLevelToBlocksImpl(ManyMemoryAggregatedDataVariants & non_empty_data, ThreadPool * thread_pool) const;

    void destroyAggregateStates(AggregateDataPtr & place, bool skip_state_func = false) const;

    void serializeAggregateStates(const AggregateDataPtr & place, WriteBuffer & wb) const;
    /// \param old_aggregates_size is used for deserialization of old aggregate states
    void deserializeAggregateStates(
        AggregateDataPtr & place, ReadBuffer & rb, Arena * arena, std::optional<size_t> old_aggregates_size = {}) const;

    /// \return true means execution must be aborted, false means normal
    bool checkAndProcessResult(MemoryAggregatedDataVariants & result) const;

    template <typename Method>
    bool executeAndRetractImpl(
        Method & method,
        MemoryAggregatedDataVariants & result,
        size_t row_begin,
        size_t row_end,
        ColumnRawPtrs & key_columns,
        AggregateFunctionInstruction * aggregate_instructions) const;

    template <bool is_two_level, typename Method, typename Table, typename KeyHandler = EmptyKeyHandler>
    void mergeUpdatesDataImpl(
        std::vector<Table *> & tables,
        const ArenaPtr & pool,
        bool use_compiled_functions,
        bool reset_updated,
        AggregatingConvertParams & cparams,
        KeyHandler && key_handler = nullptr) const;

    template <typename Method>
    void mergeRetractGroupsImpl(ManyMemoryAggregatedDataVariants & non_empty_data, Arena * arena, AggregatingConvertParams & cparams) const;

    void mergeUpdateGroups(ManyMemoryAggregatedDataVariants & non_empty_data, AggregatingConvertParams & cparams) const;

    void mergeRetractGroups(ManyMemoryAggregatedDataVariants & non_empty_data, AggregatingConvertParams & cparams) const;

    BlocksList prepareBlockAndFillSingleLevel(MemoryAggregatedDataVariants & data_variants, AggregatingConvertParams & cparams) const;

    BlocksList prepareBlocksAndFillTwoLevel(
        MemoryAggregatedDataVariants & data_variants, size_t max_threads, AggregatingConvertParams & cparams) const;

    template <typename Method>
    BlocksList prepareBlocksAndFillTwoLevelImpl(
        MemoryAggregatedDataVariants & data_variants, Method & method, ThreadPool * thread_pool, AggregatingConvertParams & cparams) const;

    template <typename Method>
    void mergeBucketImpl(
        ManyMemoryAggregatedDataVariants & data, Int64 bucket, const ArenaPtr & pool, std::atomic<bool> * is_cancelled = nullptr) const;

    template <typename Method, typename Table>
    void destroyImpl(Table & table) const;

    void destroyWithoutKey(MemoryAggregatedDataVariants & result) const;

    /** Checks constraints on the maximum number of keys for aggregation.
      * If it is exceeded, then, depending on the group_by_overflow_mode, either
      * - throws an exception;
      * - returns false, which means that execution must be aborted;
      */
    bool checkLimits(size_t result_size) const;

    void initDataVariants(MemoryAggregatedDataVariants & result) const;

    template <typename Method, typename Table>
    void mergeDataNullKey(Table & table_dst, Table & table_src, Arena * arena) const
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
                    for (size_t i = 0; i < params->aggregates_size; ++i)
                        aggregate_functions[i]->merge(
                            table_dst.getNullKeyData() + offsets_of_aggregate_states[i],
                            table_src.getNullKeyData() + offsets_of_aggregate_states[i],
                            arena);
                }
            }
        }
    }

    template <typename Method, typename Table, typename KeyHandler = EmptyKeyHandler>
    void mergeDataImpl(
        Table & table_dst, Table & table_src, const ArenaPtr & pool, bool use_compiled_functions, KeyHandler && key_handler = nullptr) const
    {
        Arena * arena = pool.get();
        if constexpr (Method::low_cardinality_optimization)
            mergeDataNullKey<Method, Table>(table_dst, table_src, arena);

#if USE_EMBEDDED_COMPILER
        PaddedPODArray<AggregateDataPtr> dst_places;
        PaddedPODArray<AggregateDataPtr> src_places;
        if (use_compiled_functions)
        {
            auto approx_size = table_src.size();
            dst_places.reserve(approx_size);
            src_places.reserve(approx_size);
        }
#endif

        auto func = [&](AggregateDataPtr & __restrict dst, AggregateDataPtr & __restrict src, bool inserted) {
            if (inserted)
            {
                /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                dst = nullptr;

                /// If there are multiple sources, there are more than one AggregatedDataVariant. MemoryAggregator always creates a new AggregatedDataVariant and merge all other
                /// MemoryAggregatedDataVariants to the new created one. After finalize(), it does not clean up aggregate state except the new create AggregatedDataVariant.
                /// If it does not alloc new memory for the 'dst' (i.e. aggregate state of the new AggregatedDataVariant which get destroyed after finalize()) but reuse
                /// that from the 'src' to store the final aggregated result, it will cause the data from other AggregatedDataVariant will be merged multiple times and
                /// generate incorrect aggregated result.
                auto * aggregate_data = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(aggregate_data);
                dst = aggregate_data;
            }

#if USE_EMBEDDED_COMPILER
            if (use_compiled_functions)
            {
                dst_places.push_back(dst);
                src_places.push_back(src);
            }
#endif

            mergeAggregateStates(dst, src, arena, /*skip_compiled_aggregate_functions=*/use_compiled_functions);
        };

        if constexpr (std::is_same_v<KeyHandler, EmptyKeyHandler>)
        {
            table_src.template mergeToViaEmplace<decltype(func), false>(table_dst, std::move(func));
        }
        else
        {
            table_src.forEachValue([&](const auto & key, auto & mapped) {
                typename Table::LookupResult res_it;
                bool inserted;
                table_dst.emplace(key_handler(key), res_it, inserted);
                func(res_it->getMapped(), mapped, inserted);
            });
        }

#if USE_EMBEDDED_COMPILER
        if (use_compiled_functions)
        {
            const auto & compiled_functions = compiled_aggregate_functions_holder->compiled_aggregate_functions;
            compiled_functions.merge_aggregate_states_function(dst_places.data(), src_places.data(), dst_places.size());
        }
#endif
    }

private:
    friend struct MemoryAggregatedDataVariants;

    /// Data structure of source blocks.
    MemoryAggregatorParamsPtr memory_params;

    MemoryAggregatedDataVariants::Type method_chosen;
    mutable std::optional<size_t> bucket_key_offset;

    HashMethodContextPtr aggregation_state_cache;

    bool has_state_functions = false;
};

}

}
