#include <Processors/Transforms/Streaming/TumbleAggregatingTransform.h>

namespace DB
{
namespace Streaming
{
TumbleAggregatingTransform::TumbleAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : TumbleAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

TumbleAggregatingTransform::TumbleAggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_)
    : WindowAggregatingTransform(
        std::move(header),
        std::move(params_),
        std::move(many_data_),
        current_variant_,
        max_threads_,
        temporary_data_merge_threads_,
        "TumbleAggregatingTransform",
        ProcessorID::TumbleAggregatingTransformID)
    , window_params(params->params.window_params->as<TumbleWindowParams &>())
{
}

WindowsWithBuckets TumbleAggregatingTransform::getLocalFinalizedWindowsWithBucketsImpl(Int64 watermark) const
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

        auto final_buckets = getBucketsBefore(max_finalized_bucket);
        for (auto time_bucket : final_buckets)
            windows_with_buckets.emplace_back(WindowWithBuckets{
                {static_cast<Int64>(time_bucket),
                 addTime(
                     static_cast<Int64>(time_bucket),
                     window_params.interval_kind,
                     window_params.window_interval,
                     *window_params.time_zone,
                     window_params.time_scale)},
                {time_bucket}});
    }
    else
    {
        auto max_finalized_bucket = current_window_start;
        auto final_buckets = getBucketsBefore(max_finalized_bucket);
        for (auto time_bucket : final_buckets)
            windows_with_buckets.emplace_back(WindowWithBuckets{
                {addTime(
                     static_cast<Int64>(time_bucket),
                     window_params.interval_kind,
                     -window_params.window_interval,
                     *window_params.time_zone,
                     window_params.time_scale),
                 static_cast<Int64>(time_bucket)},
                {time_bucket}});
    }

    return windows_with_buckets;
}

void TumbleAggregatingTransform::removeBucketsImpl(Int64 watermark)
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

    params->aggregator.removeBucketsBefore(variants, max_time_bucket_can_be_removed);
}

}
}
