#include "TumbleHopAggregatingTransform.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
TumbleHopAggregatingTransform::TumbleHopAggregatingTransform(Block header, StreamingAggregatingTransformParamsPtr params_)
    : TumbleHopAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyStreamingAggregatedData>(1), 0, 1, 1)
{
}

TumbleHopAggregatingTransform::TumbleHopAggregatingTransform(
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
        "TumbleHopAggregatingTransform")
{
    assert(
        (params->params.group_by == StreamingAggregator::Params::GroupBy::WINDOW_START)
        || (params->params.group_by == StreamingAggregator::Params::GroupBy::WINDOW_END));
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void TumbleHopAggregatingTransform::finalize(ChunkInfoPtr chunk_info)
{
    chunk_info->ctx.getWatermark(watermark_bound.watermark, watermark_bound.watermark_lower_bound);

    if (many_data->finalizations.fetch_add(1) + 1 == many_data->variants.size())
    {
        /// The current transform is the last one in this round of
        /// finalization. Do watermark alignment for all of the variants
        /// pick the smallest watermark
        WatermarkBound min_watermark{watermark_bound};
        WatermarkBound max_watermark{watermark_bound};

        for (auto & bound : many_data->watermarks)
        {
            if (bound.watermark < min_watermark.watermark)
                min_watermark = bound;

            if (bound.watermark > min_watermark.watermark)
                max_watermark = bound;

            /// Reset watermarks
            bound.watermark = 0;
            bound.watermark_lower_bound = 0;
        }

        if (min_watermark.watermark != max_watermark.watermark)
            LOG_INFO(log, "Found watermark skew. min_watermark={}, max_watermark={}", min_watermark.watermark, min_watermark.watermark);

        auto start = MonotonicMilliseconds::now();
        doFinalize(min_watermark, chunk_info);
        auto end = MonotonicMilliseconds::now();

        LOG_INFO(log, "Took {} milliseconds to finalize {} shard aggregation", end - start, many_data->variants.size());

        // Clear the finalization count
        many_data->finalizations.store(0);

        /// We are done with finalization, notify all transforms start to work again
        many_data->finalized.notify_all();

        /// We first notify all other variants that the aggregation is done for this round
        /// and then remove the project window buckets and their memory arena for the current variant.
        /// This save a bit time and a bit more efficiency because all variants can do memory arena
        /// recycling in parallel.
        removeBuckets();
    }
    else
    {
        /// Condition wait for finalization transform thread to finish the aggregation
        auto start = MonotonicMilliseconds::now();

        std::unique_lock<std::mutex> lk(many_data->finalizing_mutex);
        many_data->finalized.wait(lk);

        auto end = MonotonicMilliseconds::now();
        LOG_INFO(
            log,
            "StreamingAggregated. Took {} milliseconds to wait for finalizing {} shard aggregation",
            end - start,
            many_data->variants.size());

        removeBuckets();
    }
}

void TumbleHopAggregatingTransform::doFinalize(const WatermarkBound & watermark, ChunkInfoPtr & chunk_info)
{
    /// FIXME spill to disk, overflow_row etc cases
    auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
    auto prepared_data_ptr = std::make_shared<ManyStreamingAggregatedDataVariants>(std::move(prepared_data));

    if (prepared_data_ptr->empty())
        return;

    initialize(prepared_data_ptr);

    assert(prepared_data_ptr->at(0)->isTwoLevel());
    mergeTwoLevel(prepared_data_ptr, watermark, chunk_info);

    rows_since_last_finalization = 0;
}

void TumbleHopAggregatingTransform::initialize(ManyStreamingAggregatedDataVariantsPtr & data)
{
    StreamingAggregatedDataVariantsPtr & first = data->at(0);

    assert(first->type != StreamingAggregatedDataVariants::Type::without_key && !params->params.overflow_row);

    /// At least we need one arena in first data item per thread
    if (max_threads > first->aggregates_pools.size())
    {
        Arenas & first_pool = first->aggregates_pools;
        for (size_t j = first_pool.size(); j < max_threads; j++)
            first_pool.emplace_back(std::make_shared<Arena>());
    }
}

void TumbleHopAggregatingTransform::mergeTwoLevel(
    ManyStreamingAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, ChunkInfoPtr & chunk_info)
{
    /// FIXME, parallelization ? We simply don't know for now if parallelization makes sense since most of the time, we have only
    /// one project window for streaming processing
    auto & first = data->at(0);

    std::atomic<bool> is_cancelled{false};

    Block merged_block;

    for (size_t index = data->size() == 1 ? 0 : 1; index < first->aggregates_pools.size(); ++index)
    {
        Arena * arena = first->aggregates_pools.at(index).get();

        /// Figure out which buckets need get merged
        auto & data_variant = data->at(index);
        std::vector<size_t> buckets
            = data_variant->aggregator->bucketsBefore(*data_variant, watermark.watermark_lower_bound, watermark.watermark);

        for (auto bucket : buckets)
        {
            Block block = params->aggregator.mergeAndConvertOneBucketToBlock(*data, arena, params->final, bucket, &is_cancelled);
            if (is_cancelled)
                return;

            if (params->emit_version)
                emitVersion(block);

            if (merged_block)
            {
                assertBlocksHaveEqualStructure(merged_block, block, "merging buckets for streaming two level hashtable");
                for (size_t i = 0, size = merged_block.columns(); i < size; ++i)
                {
                    const auto source_column = block.getByPosition(i).column;
                    auto mutable_column = IColumn::mutate(std::move(merged_block.getByPosition(i).column));
                    mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
                    merged_block.getByPosition(i).column = std::move(mutable_column);
                }
            }
            else
                merged_block = std::move(block);
        }
    }

    if (merged_block)
        setCurrentChunk(convertToChunk(merged_block), chunk_info);

    /// Tell other variants to clean up memory arena
    many_data->arena_watermark = watermark;
}

/// Cleanup memory arena for the projected window buckets
void TumbleHopAggregatingTransform::removeBuckets()
{
    variants.aggregator->removeBucketsBefore(
        variants, many_data->arena_watermark.watermark_lower_bound, many_data->arena_watermark.watermark);
}

}
