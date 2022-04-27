#include "GlobalAggregatingTransform.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
GlobalAggregatingTransform::GlobalAggregatingTransform(Block header, StreamingAggregatingTransformParamsPtr params_)
    : GlobalAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyStreamingAggregatedData>(1), 0, 1, 1)
{
}

GlobalAggregatingTransform::GlobalAggregatingTransform(
    Block header,
    StreamingAggregatingTransformParamsPtr params_,
    ManyStreamingAggregatedDataPtr many_data_,
    size_t current_variant,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : StreamingAggregatingTransform(
        std::move(header),
        std::move(params_),
        std::move(many_data_),
        current_variant,
        max_threads_,
        temporary_data_merge_threads_,
        "GlobalAggregatingTransform")
{
    assert(params->params.group_by == StreamingAggregator::Params::GroupBy::OTHER);
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void GlobalAggregatingTransform::finalize(ChunkInfoPtr chunk_info)
{
    if (many_data->finalizations.fetch_add(1) + 1 == many_data->variants.size())
    {
        auto start = MonotonicMilliseconds::now();
        doFinalize(chunk_info);
        auto end = MonotonicMilliseconds::now();

        LOG_INFO(log, "Took {} milliseconds to finalize {} shard aggregation", end - start, many_data->variants.size());

        // Clear the finalization count
        many_data->finalizations.store(0);

        /// We are done with finalization, notify all transforms start to work again
        many_data->finalized.notify_all();
    }
    else
    {
        /// Condition wait for finalization transform thread to finish the aggregation
        auto start = MonotonicMilliseconds::now();

        std::unique_lock<std::mutex> lk(many_data->finalizing_mutex);
        many_data->finalized.wait(lk);

        auto end = MonotonicMilliseconds::now();
        LOG_INFO(log, "Took {} milliseconds to wait for finalizing {} shard aggregation", end - start, many_data->variants.size());
    }
}

void GlobalAggregatingTransform::doFinalize(ChunkInfoPtr & chunk_info)
{
    /// FIXME spill to disk, overflow_row etc cases
    auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
    auto prepared_data_ptr = std::make_shared<ManyStreamingAggregatedDataVariants>(std::move(prepared_data));

    if (prepared_data_ptr->empty())
        return;

    initialize(prepared_data_ptr, chunk_info);

    if (prepared_data_ptr->at(0)->isTwoLevel())
        mergeTwoLevel(prepared_data_ptr, chunk_info);
    else
        mergeSingleLevel(prepared_data_ptr, chunk_info);

    rows_since_last_finalization = 0;
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::initialize
void GlobalAggregatingTransform::initialize(ManyStreamingAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info)
{
    StreamingAggregatedDataVariantsPtr & first = data->at(0);

    /// At least we need one arena in first data item per thread
    if (max_threads > first->aggregates_pools.size())
    {
        Arenas & first_pool = first->aggregates_pools;
        for (size_t j = first_pool.size(); j < max_threads; j++)
            first_pool.emplace_back(std::make_shared<Arena>());
    }

    if (first->type == StreamingAggregatedDataVariants::Type::without_key || params->params.overflow_row)
    {
        params->aggregator.mergeWithoutKeyDataImpl(*data);
        auto block = params->aggregator.prepareBlockAndFillWithoutKey(
            *first, params->final, first->type != StreamingAggregatedDataVariants::Type::without_key);

        setCurrentChunk(convertToChunk(block), chunk_info);
    }
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::mergeSingleLevel
void GlobalAggregatingTransform::mergeSingleLevel(ManyStreamingAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info)
{
    StreamingAggregatedDataVariantsPtr & first = data->at(0);

    if (first->type == StreamingAggregatedDataVariants::Type::without_key)
        return;

#define M(NAME) \
    else if (first->type == StreamingAggregatedDataVariants::Type::NAME) \
        params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data);
    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    auto block = params->aggregator.prepareBlockAndFillSingleLevel(*first, params->final);

    /// Tell other variants to clean up memory arena
    /// many_data->arena_watermark = watermark;

    if (params->emit_version)
        emitVersion(block);

    setCurrentChunk(convertToChunk(block), chunk_info);
}

void GlobalAggregatingTransform::mergeTwoLevel(ManyStreamingAggregatedDataVariantsPtr & data, ChunkInfoPtr & chunk_info)
{
    /// FIXME
    (void)data;
    (void)chunk_info;
}

}
