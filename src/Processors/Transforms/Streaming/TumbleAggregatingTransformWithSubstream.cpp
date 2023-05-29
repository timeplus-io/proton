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
