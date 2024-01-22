#include <Processors/Transforms/Streaming/TumbleAggregatingTransformWithSubstream.h>

namespace DB
{
namespace Streaming
{
TumbleAggregatingTransformWithSubstream::TumbleAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_)
    : WindowAggregatingTransformWithSubstream(
        std::move(header),
        std::move(params_),
        "TumbleAggregatingTransformWithSubstream",
        ProcessorID::TumbleAggregatingTransformWithSubstreamID)
    , window_params(params->params.window_params->as<TumbleWindowParams &>())
{
}

WindowsWithBuckets
TumbleAggregatingTransformWithSubstream::getFinalizedWindowsWithBuckets(Int64 watermark, const SubstreamContextPtr & substream_ctx) const
{
    if (unlikely(watermark == INVALID_WATERMARK))
        return {}; /// No window

    WindowsWithBuckets windows_with_buckets;

    /// When watermark reached to the current window, there may still be some events within the current window will arrive in future
    /// So we can project some windows before the current window.
    /// `last_finalized_window_start <=> current_window_start - window_interval`
    /// `last_finalized_window_end <=> current_window_start`
    auto current_window_start = toStartTime(
        watermark, window_params.interval_kind, window_params.window_interval, *window_params.time_zone, window_params.time_scale);
    if (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START)
    {
        auto max_finalized_bucket = addTime(
            current_window_start,
            window_params.interval_kind,
            -window_params.window_interval,
            *window_params.time_zone,
            window_params.time_scale);

        const auto & final_buckets = params->aggregator.bucketsBefore(substream_ctx->variants, max_finalized_bucket);
        for (auto time_bucket : final_buckets)
            windows_with_buckets.emplace_back(WindowWithBuckets{
                {time_bucket,
                 addTime(
                     time_bucket,
                     window_params.interval_kind,
                     window_params.window_interval,
                     *window_params.time_zone,
                     window_params.time_scale)},
                {time_bucket}});
    }
    else
    {
        auto max_finalized_bucket = current_window_start;
        const auto & final_buckets = params->aggregator.bucketsBefore(substream_ctx->variants, max_finalized_bucket);
        for (auto time_bucket : final_buckets)
            windows_with_buckets.emplace_back(WindowWithBuckets{
                {addTime(
                     time_bucket,
                     window_params.interval_kind,
                     -window_params.window_interval,
                     *window_params.time_zone,
                     window_params.time_scale),
                 time_bucket},
                {time_bucket}});
    }

    if (params->fill_missing_window)
    {
        /// If no finalized window, we should start from the current first window
        Int64 next_window_end;
        if (substream_ctx->finalized_watermark == INVALID_WATERMARK)
        {
            if (windows_with_buckets.empty())
                return {};

            next_window_end = windows_with_buckets.front().window.end;
        }
        else
        {
            auto finalized_window_start = toStartTime(
                substream_ctx->finalized_watermark,
                window_params.interval_kind,
                window_params.window_interval,
                *window_params.time_zone,
                window_params.time_scale);

            next_window_end = addTime(
                finalized_window_start,
                window_params.interval_kind,
                2 * window_params.window_interval,
                *window_params.time_zone,
                window_params.time_scale);
        }

        auto it = windows_with_buckets.begin();
        while (next_window_end <= watermark)
        {
            /// Add missing window if not exist
            if (it == windows_with_buckets.end() || next_window_end != it->window.end) [[unlikely]]
            {
                Window missing_window
                    = {addTime(
                           next_window_end,
                           window_params.interval_kind,
                           -window_params.window_interval,
                           *window_params.time_zone,
                           window_params.time_scale),
                       next_window_end};
                it = windows_with_buckets.insert(it, WindowWithBuckets{.window = std::move(missing_window), .buckets = {}});
            }

            next_window_end = addTime(
                next_window_end,
                window_params.interval_kind,
                window_params.window_interval,
                *window_params.time_zone,
                window_params.time_scale);

            ++it;
        }
    }

    return windows_with_buckets;
}

void TumbleAggregatingTransformWithSubstream::removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx)
{
    size_t max_time_bucket_can_be_removed = 0;
    /// When watermark reached to the current window, there may still be some events within the current window will arrive in future
    /// So we can project some windows before the current window.
    /// `last_finalized_window_start <=> current_window_start - window_interval`
    /// `last_finalized_window_end <=> current_window_start`
    auto current_window_start = toStartTime(
        watermark, window_params.interval_kind, window_params.window_interval, *window_params.time_zone, window_params.time_scale);
    if (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START)
        max_time_bucket_can_be_removed = addTime(
            current_window_start,
            window_params.interval_kind,
            -window_params.window_interval,
            *window_params.time_zone,
            window_params.time_scale);
    else
        max_time_bucket_can_be_removed = current_window_start;

    params->aggregator.removeBucketsBefore(substream_ctx->variants, max_time_bucket_can_be_removed);
}

}
}
