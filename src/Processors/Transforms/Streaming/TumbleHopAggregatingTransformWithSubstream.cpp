#include "TumbleHopAggregatingTransformWithSubstream.h"

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{

TumbleHopAggregatingTransformWithSubstream::TumbleHopAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_)
    : TumbleHopAggregatingTransformWithSubstream(std::move(header), std::move(params_), std::make_shared<SubstreamManyAggregatedData>(1), 0, 1, 1)
{
}

TumbleHopAggregatingTransformWithSubstream::TumbleHopAggregatingTransformWithSubstream(
    Block header,
    AggregatingTransformParamsPtr params_,
    SubstraemManyAggregatedDataPtr substream_many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : AggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        std::move(substream_many_data_),
        current_variant_,
        max_threads_,
        temporary_data_merge_threads_,
        "TumbleHopAggregatingTransformWithSubstream",
        ProcessorID::TumbleHopAggregatingTransformWithSubstreamID)
{
    assert(
        (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START)
        || (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END));
}

void TumbleHopAggregatingTransformWithSubstream::consume(Chunk chunk)
{
    const UInt64 num_rows = chunk.getNumRows();

    if (num_rows > 0)
    {
        Columns columns = chunk.detachColumns();
        assert(chunk.hasChunkContext());
        executeOrMergeColumns(columns, chunk.getChunkContext()->getSubStreamID());
    }

    if (chunk.hasWatermark())
        finalize(chunk.getChunkContext());
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void TumbleHopAggregatingTransformWithSubstream::finalize(ChunkContextPtr chunk_ctx)
{
    auto watermark = chunk_ctx->getWatermark();
    SubstreamContextPtr substream_ctx = getSubstreamContext(watermark.id);
    auto [min_watermark, max_watermark, arena_watermark] = finalizeAndGetWatermarks(substream_ctx, watermark);

    /// Do memory arena recycling by last finalized watermark
    if (arena_watermark.valid())
        removeBuckets(substream_ctx, arena_watermark);

    /// Finalize current watermark
    if (min_watermark.valid())
    {
        assert(max_watermark.valid());
        if (min_watermark.watermark != max_watermark.watermark)
            LOG_WARNING(
                log,
                "Found watermark skew in substream id={}. min_watermark={}, max_watermark={}",
                min_watermark.id,
                min_watermark.watermark,
                max_watermark.watermark);

        auto start = MonotonicMilliseconds::now();
        doFinalize(min_watermark, chunk_ctx);
        auto end = MonotonicMilliseconds::now();

        LOG_DEBUG(
            log,
            "Took {} milliseconds to finalize {} shard aggregation in substream id={}. watermark={}",
            end - start,
            many_aggregating_size,
            min_watermark.id,
            min_watermark.watermark);

        // Clear the finalizing count and set arena recycle watermark
        {
            std::lock_guard<std::mutex> lock(substream_ctx->finalizing_mutex);
            substream_ctx->finalized.reset();

            /// We first notify all other variants that the aggregation is done for this round
            /// and then remove the project window buckets and their memory arena for the current variant.
            /// This save a bit time and a bit more efficiency because all variants can do memory arena
            /// recycling in parallel.
            substream_ctx->arena_watermark = min_watermark;
        }
    }
}

void TumbleHopAggregatingTransformWithSubstream::doFinalize(const WatermarkBound & watermark, ChunkContextPtr & chunk_ctx)
{
    /// FIXME spill to disk, overflow_row etc cases
    auto substream_ctx = getSubstreamContext(watermark.id);

    /// We lock all variants of current substream to merge
    std::lock_guard lock(substream_ctx->variants_mutex);
    auto prepared_data_ptr = params->aggregator.prepareVariantsToMerge(substream_ctx->many_variants);
    if (prepared_data_ptr->empty())
        return;

    initialize(prepared_data_ptr);

    assert(prepared_data_ptr->at(0)->isTwoLevel());
    convertTwoLevel(prepared_data_ptr, watermark, chunk_ctx);
}

void TumbleHopAggregatingTransformWithSubstream::initialize(ManyAggregatedDataVariantsPtr & data)
{
    AggregatedDataVariantsPtr & first = data->at(0);

    assert(first->type != AggregatedDataVariants::Type::without_key && !params->params.overflow_row);

    /// At least we need one arena in first data item per thread
    Arenas & first_pool = first->aggregates_pools;
    for (size_t j = first_pool.size(); j < max_threads; j++)
        first_pool.emplace_back(std::make_shared<Arena>());
}

void TumbleHopAggregatingTransformWithSubstream::convertTwoLevel(
    ManyAggregatedDataVariantsPtr & data, const WatermarkBound & watermark, ChunkContextPtr & chunk_ctx)
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
        std::vector<size_t> buckets = data_variant->aggregator->bucketsBefore(*data_variant, watermark);

        for (auto bucket : buckets)
        {
            Block block = params->aggregator.mergeAndConvertOneBucketToBlock(*data, arena, params->final, ConvertAction::STREAMING_EMIT, bucket, &is_cancelled);
            if (is_cancelled)
                return;

            if (params->emit_version)
                emitVersion(block, watermark.id);

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
        setCurrentChunk(convertToChunk(merged_block), chunk_ctx);
}

void TumbleHopAggregatingTransformWithSubstream::removeBuckets(SubstreamContextPtr substream_ctx, const WatermarkBound & watermark)
{
    std::shared_lock lock(substream_ctx->variants_mutex);
    params->aggregator.removeBucketsBefore(*(substream_ctx->many_variants[current_variant]), watermark);
}

std::tuple<WatermarkBound, WatermarkBound, WatermarkBound>
TumbleHopAggregatingTransformWithSubstream::finalizeAndGetWatermarks(SubstreamContextPtr substream_ctx, const WatermarkBound & wb)
{
    WatermarkBound min_watermark, max_watermark, arena_watermark;
    std::lock_guard lock(substream_ctx->finalizing_mutex);
    /// Update min/max watermark for current substream
    if (wb.watermark < substream_ctx->min_watermark.watermark)
        substream_ctx->min_watermark = wb;

    if (wb.watermark > substream_ctx->max_watermark.watermark)
        substream_ctx->max_watermark = wb;

    /// Set finalized status for one part of current substream,
    /// and return min/max watermark that requires finalizing
    substream_ctx->finalized.set(current_variant);
    if (substream_ctx->finalized == all_finalized_mark)
    {
        min_watermark = substream_ctx->min_watermark;
        max_watermark = substream_ctx->max_watermark;
        substream_ctx->min_watermark = MAX_WATERMARK;
        substream_ctx->max_watermark = MIN_WATERMARK;
    }

    /// Return watermark that requires arena recycling
    /// (ignored the same watermark in same substream)
    if (prev_arena_watermark != substream_ctx->arena_watermark)
    {
        arena_watermark = substream_ctx->arena_watermark;
        prev_arena_watermark = arena_watermark;
    }

    return {min_watermark, max_watermark, arena_watermark};
}
}
}
