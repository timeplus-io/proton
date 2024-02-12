#include <Processors/Transforms/Streaming/WindowAggregatingTransform.h>

#include <Processors/Transforms/Streaming/AggregatingHelper.h>
#include <Processors/Transforms/convertToChunk.h>

#include <ranges>

namespace DB
{
namespace Streaming
{
WindowAggregatingTransform::WindowAggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
    const String & log_name,
    ProcessorID pid_)
    : AggregatingTransform(
        std::move(header),
        std::move(params_),
        std::move(many_data_),
        current_variant_,
        max_threads_,
        temporary_data_merge_threads_,
        log_name,
        pid_)
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

bool WindowAggregatingTransform::needFinalization(Int64 min_watermark) const
{
    if (only_emit_finalized_windows && min_watermark <= many_data->finalized_watermark.load(std::memory_order_relaxed))
        return false;

    auto local_windows_with_buckets = getLocalWindowsWithBucketsImpl();

    /// We need to remove some invalid windows, when:
    /// 1) some lagged events arrived after timeout, we skip finalized windows and remove them later
    /// 2) if we only emit finalized windows, skip imtermidate windows
    auto last_finalized_window_end = many_data->finalized_window_end.load(std::memory_order_relaxed);
    for (auto iter = local_windows_with_buckets.begin(); iter != local_windows_with_buckets.end();)
    {
        bool is_invalid_window = iter->window.end <= last_finalized_window_end;
        is_invalid_window |= only_emit_finalized_windows && iter->window.end > min_watermark;
        if (is_invalid_window)
            iter = local_windows_with_buckets.erase(iter);
        else
            ++iter;
    }

    /// Has windows to finalize
    return !local_windows_with_buckets.empty();
}

bool WindowAggregatingTransform::prepareFinalization(Int64 min_watermark)
{
    /// If we only emit finalized windows and the watermark has been finalized in by another AggregatingTransform, skip it
    if (only_emit_finalized_windows && min_watermark <= many_data->finalized_watermark.load(std::memory_order_relaxed))
        return false;

    /// FIXME: For multiple WindowAggregatingTransform, will emit multiple times in a periodic interval,
    /// we can do periodic emit in AggregatingTransform, not in WatermarkTransform.
    if ((params->emit_mode == EmitMode::PeriodicWatermark || params->emit_mode == Streaming::EmitMode::PeriodicWatermarkOnUpdate) && !many_data->hasNewData())
        return false;

    /// After acquired finalizing lock
    prepared_windows_with_buckets.clear();
    for (auto * aggr_transform : many_data->aggregating_transforms)
    {
        auto * window_aggr_transform = reinterpret_cast<WindowAggregatingTransform *>(aggr_transform);
        auto windows_with_buckets = window_aggr_transform->getLocalWindowsWithBucketsImpl();

        /// In case when some lagged events arrived after timeout, we skip finalized windows and remove them later
        auto last_finalized_window_end = many_data->finalized_window_end.load(std::memory_order_relaxed);
        for (auto iter = windows_with_buckets.begin(); iter != windows_with_buckets.end();)
        {
            bool is_invalid_window = iter->window.end <= last_finalized_window_end;
            is_invalid_window |= only_emit_finalized_windows && iter->window.end > min_watermark;
            if (is_invalid_window)
                iter = windows_with_buckets.erase(iter);
            else
                ++iter;
        }

        for (auto & window_with_buckets : windows_with_buckets)
        {
            auto iter = prepared_windows_with_buckets.begin();
            for (; iter != prepared_windows_with_buckets.end(); ++iter)
            {
                if (iter->window.end == window_with_buckets.window.end)
                {
                    /// Unique merge buckets of same window (assume buckets always are sorted)
                    assert(iter->window.start == window_with_buckets.window.start);
                    std::vector<Int64> buckets;
                    buckets.reserve(std::max(iter->buckets.size(), window_with_buckets.buckets.size()));
                    std::ranges::set_union(iter->buckets, window_with_buckets.buckets, std::back_inserter(buckets));
                    iter->buckets = std::move(buckets);
                    break;
                }
                else if (iter->window.end > window_with_buckets.window.end)
                {
                    /// Merge a new window
                    iter = prepared_windows_with_buckets.emplace(iter, std::move(window_with_buckets));
                    break;
                }
            }

            /// Merge a new window into the back
            if (iter == prepared_windows_with_buckets.end())
                prepared_windows_with_buckets.push_back(std::move(window_with_buckets));
        }
    }

    /// Has windows to finalize
    return !prepared_windows_with_buckets.empty();
}

/// Finalize what we have in memory and produce a finalized Block
/// and push the block to downstream pipe
void WindowAggregatingTransform::finalize(const ChunkContextPtr & chunk_ctx)
{
    assert(chunk_ctx && chunk_ctx->hasWatermark());

    SCOPE_EXIT({ many_data->resetRowCounts(); });

    /// FIXME, parallelization ? We simply don't know for now if parallelization makes sense since most of the time, we have only
    /// one project window for streaming processing
    Chunk merged_chunk;
    Chunk chunk;

    assert(!prepared_windows_with_buckets.empty());
    for (const auto & window_with_buckets : prepared_windows_with_buckets)
    {
        if (only_emit_updates)
            chunk = AggregatingHelper::mergeAndSpliceAndConvertUpdatesToChunk(many_data->variants, *params, window_with_buckets.buckets);
        else
            chunk = AggregatingHelper::mergeAndSpliceAndConvertToChunk(many_data->variants, *params, window_with_buckets.buckets);

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
            emitVersion(chunk);

        merged_chunk.append(std::move(chunk));
    }

    /// `mergeAndSpliceAndConvertUpdatesToChunk` only converts updates but doesn't reset updated flags,
    /// we need to manually reset them after all windows conversions.
    if (only_emit_updates)
    {
        for (const auto & window_with_buckets : prepared_windows_with_buckets)
            for (auto & variants : many_data->variants)
                params->aggregator.resetUpdatedForBuckets(*variants, window_with_buckets.buckets);
    }

    merged_chunk.setChunkContext(chunk_ctx);
    auto finalized_watermark = chunk_ctx->getWatermark();
    for (const auto & window_with_buckets : prepared_windows_with_buckets | std::views::reverse)
    {
        /// If there are only intermediate windows, then just emit but not finalize
        if (window_with_buckets.window.end <= finalized_watermark)
        {
            many_data->finalized_window_end.store(window_with_buckets.window.end, std::memory_order_relaxed);
            break;
        }
    }
    prepared_windows_with_buckets.clear();
    prepared_windows_with_buckets.shrink_to_fit();

    /// If is timeout, we set watermark after actual finalized last window end
    if (unlikely(finalized_watermark == TIMEOUT_WATERMARK))
    {
        finalized_watermark = many_data->finalized_window_end.load(std::memory_order_relaxed);
        merged_chunk.setWatermark(finalized_watermark);
    }

    many_data->finalized_watermark.store(finalized_watermark, std::memory_order_relaxed);

    assert(merged_chunk.getWatermark() == finalized_watermark);
    setCurrentChunk(std::move(merged_chunk));
}

void WindowAggregatingTransform::clearExpiredState(Int64 finalized_watermark)
{
    /// Blocking finalization during remove buckets from current variant
    std::lock_guard lock(variants_mutex);
    removeBucketsImpl(finalized_watermark);
}

std::vector<Int64> WindowAggregatingTransform::getBuckets() const
{
    /// Blocking finalization, it's a lightweight lock
    std::lock_guard lock(variants_mutex);
    return params->aggregator.buckets(variants);
}

}
}
