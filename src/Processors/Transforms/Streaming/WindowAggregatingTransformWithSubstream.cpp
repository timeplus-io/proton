#include <Processors/Transforms/Streaming/WindowAggregatingTransformWithSubstream.h>

#include <Processors/Transforms/Streaming/AggregatingHelper.h>
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

    const auto & output_header = getOutputs().front().getHeader();
    if (output_header.has(ProtonConsts::STREAMING_WINDOW_START))
        window_start_col_pos = output_header.getPositionByName(ProtonConsts::STREAMING_WINDOW_START);

    if (output_header.has(ProtonConsts::STREAMING_WINDOW_END))
        window_end_col_pos = output_header.getPositionByName(ProtonConsts::STREAMING_WINDOW_END);
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

    Chunk merged_chunk;
    Chunk chunk;

    const auto & last_finalized_windows_with_buckets = getFinalizedWindowsWithBuckets(substream_ctx->finalized_watermark, substream_ctx);
    const auto & windows_with_buckets = getFinalizedWindowsWithBuckets(watermark, substream_ctx);
    for (const auto & window_with_buckets : windows_with_buckets)
    {
        /// In case when some lagged events arrived after timeout, we skip the finalized windows
        if (!last_finalized_windows_with_buckets.empty()
            && window_with_buckets.window.end <= last_finalized_windows_with_buckets.back().window.end)
            continue;

        chunk = AggregatingHelper::spliceAndConvertBucketsToChunk(data_variant, *params, window_with_buckets.buckets);

        if (needReassignWindow())
            reassignWindow(chunk, window_with_buckets.window, params->params.window_params->time_col_is_datetime64, window_start_col_pos, window_end_col_pos);

        if (params->emit_version && params->final)
            emitVersion(chunk, substream_ctx);

        merged_chunk.append(std::move(chunk));
    }

    if (watermark != TIMEOUT_WATERMARK)
        substream_ctx->finalized_watermark = watermark;
    /// If is timeout, we set watermark after actual finalized last window
    else if (!windows_with_buckets.empty())
        substream_ctx->finalized_watermark = windows_with_buckets.back().window.end;

    if (merged_chunk)
    {
        chunk_ctx->setWatermark(substream_ctx->finalized_watermark);
        setCurrentChunk(std::move(merged_chunk), chunk_ctx);
    }
}

}
}
