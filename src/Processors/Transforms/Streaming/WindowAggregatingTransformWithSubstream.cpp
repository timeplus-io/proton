#include <Processors/Transforms/Streaming/WindowAggregatingTransformWithSubstream.h>

#include <Processors/Transforms/convertToChunk.h>

namespace DB
{
namespace Streaming
{
WindowAggregatingTransformWithSubstream::WindowAggregatingTransformWithSubstream(
    Block header, AggregatingTransformParamsPtr params_, const String & log_name, ProcessorID pid_)
    : AggregatingTransformWithSubstream(std::move(header), std::move(params_), log_name, pid_)
{
    assert(params->params.window_params);
    assert(
        params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
        || params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END);
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void WindowAggregatingTransformWithSubstream::finalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx)
{
    assert(substream_ctx);

    SCOPE_EXIT({ substream_ctx->resetRowCounts(); });

    /// Finalize current watermark
    auto watermark = chunk_ctx->getWatermark();
    auto start = MonotonicMilliseconds::now();
    doFinalize(watermark, substream_ctx, chunk_ctx);
    auto end = MonotonicMilliseconds::now();

    LOG_INFO(
        log,
        "Took {} milliseconds to finalize aggregation in substream id={}. finalized_watermark={}",
        end - start,
        substream_ctx->id,
        substream_ctx->finalized_watermark);

    /// Do memory arena recycling by last finalized watermark
    removeBucketsImpl(substream_ctx->finalized_watermark, substream_ctx);
}

void WindowAggregatingTransformWithSubstream::doFinalize(
    Int64 watermark, const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx)
{
    assert(substream_ctx);

    auto & data_variant = substream_ctx->variants;
    if (data_variant.empty())
        return;

    assert(data_variant.isTwoLevel());

    Block merged_block;

    Block block;

    const auto & last_finalized_windows_with_buckets = getFinalizedWindowsWithBuckets(substream_ctx->finalized_watermark, substream_ctx);
    const auto & windows_with_buckets = getFinalizedWindowsWithBuckets(watermark, substream_ctx);
    for (const auto & window_with_buckets : windows_with_buckets)
    {
        /// In case when some lagged events arrived after timeout, we skip the finalized windows
        if (!last_finalized_windows_with_buckets.empty()
            && window_with_buckets.window.end <= last_finalized_windows_with_buckets.back().window.end)
            continue;

        if (window_with_buckets.buckets.size() == 1)
        {
            if (params->final)
                block = params->aggregator.convertOneBucketToBlockFinal(
                    data_variant, ConvertAction::STREAMING_EMIT, window_with_buckets.buckets[0]);
            else
                block = params->aggregator.convertOneBucketToBlockIntermediate(
                    data_variant, ConvertAction::STREAMING_EMIT, window_with_buckets.buckets[0]);
        }
        else
        {
            block = params->aggregator.spliceAndConvertBucketsToBlock(
                data_variant, params->final, ConvertAction::INTERNAL_MERGE, window_with_buckets.buckets);
        }

        if (needReassignWindow())
            reassignWindow(block, window_with_buckets.window);

        if (params->emit_version && params->final)
            emitVersion(block, substream_ctx);

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

    if (watermark != TIMEOUT_WATERMARK)
        substream_ctx->finalized_watermark = watermark;
    /// If is timeout, we set watermark after actual finalized last window
    else if (!windows_with_buckets.empty())
        substream_ctx->finalized_watermark = windows_with_buckets.back().window.end;

    if (merged_block)
    {
        chunk_ctx->setWatermark(substream_ctx->finalized_watermark);
        setCurrentChunk(convertToChunk(merged_block), chunk_ctx);
    }
}

}
}
