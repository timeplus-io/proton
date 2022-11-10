#include "GlobalAggregatingTransform.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
GlobalAggregatingTransform::GlobalAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : GlobalAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

GlobalAggregatingTransform::GlobalAggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : AggregatingTransform(
        std::move(header),
        std::move(params_),
        std::move(many_data_),
        current_variant_,
        max_threads_,
        temporary_data_merge_threads_,
        "GlobalAggregatingTransform",
        ProcessorID::GlobalAggregatingTransformID)
{
    assert(params->params.group_by == Aggregator::Params::GroupBy::OTHER);
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void GlobalAggregatingTransform::finalize(ChunkContextPtr chunk_ctx)
{
    if (many_data->finalizations.fetch_add(1) + 1 == many_data->variants.size())
    {
        auto start = MonotonicMilliseconds::now();
        doFinalize(chunk_ctx);
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

void GlobalAggregatingTransform::doFinalize(ChunkContextPtr & chunk_ctx)
{
    /// FIXME spill to disk, overflow_row etc cases
    auto prepared_data_ptr = params->aggregator.prepareVariantsToMerge(many_data->variants);
    if (prepared_data_ptr->empty())
        return;

    SCOPE_EXIT({ rows_since_last_finalization = 0; });

    if (initialize(prepared_data_ptr, chunk_ctx))
        /// Processed
        return;

    if (prepared_data_ptr->at(0)->isTwoLevel())
        convertTwoLevel(prepared_data_ptr, chunk_ctx);
    else
        convertSingleLevel(prepared_data_ptr, chunk_ctx);
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::initialize
bool GlobalAggregatingTransform::initialize(ManyAggregatedDataVariantsPtr & data, ChunkContextPtr & chunk_ctx)
{
    AggregatedDataVariantsPtr & first = data->at(0);

    /// At least we need one arena in first data item per thread. FIXME, we are using 1 thread to do state merge
    //    Arenas & first_pool = first->aggregates_pools;
    //    for (size_t j = first_pool.size(); j < max_threads; j++)
    //        first_pool.emplace_back(std::make_shared<Arena>());

    if (first->type == AggregatedDataVariants::Type::without_key || params->params.overflow_row)
    {
        params->aggregator.mergeWithoutKeyDataImpl(*data);
        auto block = params->aggregator.prepareBlockAndFillWithoutKey(
            *first, params->final, first->type != AggregatedDataVariants::Type::without_key, ConvertAction::STREAMING_EMIT);

        if (params->emit_version)
            emitVersion(block);

        setCurrentChunk(convertToChunk(block), chunk_ctx);
        return true;
    }

    return false;
}

/// Logic borrowed from ConvertingAggregatedToChunksTransform::mergeSingleLevel
void GlobalAggregatingTransform::convertSingleLevel(ManyAggregatedDataVariantsPtr & data, ChunkContextPtr & chunk_ctx)
{
    AggregatedDataVariantsPtr & first = data->at(0);

    /// Without key aggregation is already handled in `::initialize(...)`
    assert(first->type != AggregatedDataVariants::Type::without_key);

#define M(NAME) \
    else if (first->type == AggregatedDataVariants::Type::NAME) \
        params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data, ConvertAction::STREAMING_EMIT);
    if (false)
    {
    } // NOLINT
    APPLY_FOR_VARIANTS_SINGLE_LEVEL_STREAMING(M)
#undef M
    else throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    auto block = params->aggregator.prepareBlockAndFillSingleLevel(*first, params->final, ConvertAction::STREAMING_EMIT);

    if (params->emit_version)
        emitVersion(block);

    setCurrentChunk(convertToChunk(block), chunk_ctx);
}

void GlobalAggregatingTransform::convertTwoLevel(ManyAggregatedDataVariantsPtr & data, ChunkContextPtr & chunk_ctx)
{
    /// FIXME
    (void)chunk_ctx;
    if (data)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Two level merge is not implemented in global aggregation");
}

}
}
