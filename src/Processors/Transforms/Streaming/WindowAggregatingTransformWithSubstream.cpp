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

    only_emit_finalized_windows = AggregatingHelper::onlyEmitFinalizedWindows(params->emit_mode);
    only_emit_updates = AggregatingHelper::onlyEmitUpdates(params->emit_mode);
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void WindowAggregatingTransformWithSubstream::finalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx)
{
    assert(substream_ctx);

    auto finalized_watermark = chunk_ctx->getWatermark();
    SCOPE_EXIT({
        substream_ctx->resetRowCounts();
        substream_ctx->finalized_watermark = finalized_watermark;
    });

    if ((params->emit_mode == Streaming::EmitMode::PeriodicWatermark || params->emit_mode == Streaming::EmitMode::PeriodicWatermarkOnUpdate) && !substream_ctx->hasNewData())
        return;

    /// Finalize current watermark
    auto start = MonotonicMilliseconds::now();
    doFinalize(finalized_watermark, substream_ctx, chunk_ctx);
    auto end = MonotonicMilliseconds::now();

    LOG_INFO(
        log,
        "Took {} milliseconds to finalize aggregation in substream id={}. finalized_watermark={}",
        end - start,
        substream_ctx->id,
        substream_ctx->finalized_watermark);
}

void WindowAggregatingTransformWithSubstream::doFinalize(
    Int64 watermark, const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx)
{
    assert(substream_ctx);

    auto & data_variant = substream_ctx->variants;
    if (data_variant.empty())
        return;

    assert(data_variant.isTwoLevel());

    Chunk chunk;

    const auto & last_finalized_windows = getLastFinalizedWindow(substream_ctx);
    const auto & windows_with_buckets = getWindowsWithBuckets(substream_ctx);

    ChunkList res_chunks;
    for (const auto & window_with_buckets : windows_with_buckets)
    {
        /// In case when some lagged events arrived after timeout, we skip the finalized windows
        if (last_finalized_windows.isValid() && window_with_buckets.window.end <= last_finalized_windows.end)
            continue;

        if (only_emit_finalized_windows && window_with_buckets.window.end > watermark)
            continue;

        if (only_emit_updates)
            chunk = AggregatingHelper::spliceAndConvertUpdatesToChunk(data_variant, *params, window_with_buckets.buckets);
        else
            chunk = AggregatingHelper::spliceAndConvertToChunk(data_variant, *params, window_with_buckets.buckets);

        if (!chunk)
            continue;

        if (needReassignWindow())
            reassignWindow(
                chunk,
                window_with_buckets.window,
                params->params.window_params->time_col_is_datetime64,
                window_start_col_pos,
                window_end_col_pos);

        if (params->emit_version && params->final)
            emitVersion(chunk, substream_ctx);

        res_chunks.emplace_back(std::move(chunk));
    }

    /// `spliceAndConvertUpdatesToChunk` only converts updates but doesn't reset updated flags,
    /// we need to manually reset them after all windows conversions.
    if (only_emit_updates)
    {
        for (const auto & window_with_buckets : windows_with_buckets)
            params->aggregator.resetUpdatedForBuckets(data_variant, window_with_buckets.buckets);
    }

    if (res_chunks.empty()) [[unlikely]]
        res_chunks.emplace_back(getOutputs().front().getHeader().getColumns(), 0);

    auto & last_res_chunk = res_chunks.back();
    last_res_chunk.setChunkContext(chunk_ctx);
    substream_ctx->finalized_watermark = watermark;
    /// If is timeout, we set watermark after actual finalized last window
    if (unlikely(watermark == TIMEOUT_WATERMARK && !windows_with_buckets.empty()))
    {
        substream_ctx->finalized_watermark = windows_with_buckets.back().window.end;
        last_res_chunk.setWatermark(substream_ctx->finalized_watermark);
    }

    assert(last_res_chunk.getWatermark() == substream_ctx->finalized_watermark);
    setAggregatedResult(std::move(res_chunks));
}

void WindowAggregatingTransformWithSubstream::clearExpiredState(Int64 finalized_watermark, const SubstreamContextPtr & substream_ctx)
{
    removeBucketsImpl(finalized_watermark, substream_ctx);
}

}
}
