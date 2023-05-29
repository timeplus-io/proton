#include <Processors/Transforms/Streaming/HopAggregatingTransform.h>
#include <Processors/Transforms/Streaming/HopHelper.h>

namespace DB
{
namespace Streaming
{
HopAggregatingTransform::HopAggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : HopAggregatingTransform(std::move(header), std::move(params_), std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

HopAggregatingTransform::HopAggregatingTransform(
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
        "HopAggregatingTransform",
        ProcessorID::HopAggregatingTransformID)
    , window_params(params->params.window_params->as<HopWindowParams &>())
{
}

WindowsWithBuckets HopAggregatingTransform::getFinalizedWindowsWithBuckets(Int64 watermark) const
{
    if (unlikely(watermark == INVALID_WATERMARK))
        return {}; /// No window

    Window window;
    Int64 min_bucket_of_window, max_bucket_of_window;
    auto calc_window_min_max_buckets = [&]() {
        if (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START)
        {
            min_bucket_of_window = window.start;
            max_bucket_of_window = addTime(
                window.end, window_params.interval_kind, -window_params.gcd_interval, *window_params.time_zone, window_params.time_scale);
        }
        else
        {
            min_bucket_of_window = addTime(
                window.start, window_params.interval_kind, window_params.gcd_interval, *window_params.time_zone, window_params.time_scale);
            max_bucket_of_window = window.end;
        }
    };

    /// Initial first window
    window = HopHelper::getLastFinalizedWindow(watermark, window_params);
    calc_window_min_max_buckets();

    /// Get final buckets
    std::set<Int64> final_buckets;
    for (const auto & data_variant : many_data->variants)
        for (auto bucket : params->aggregator.bucketsBefore(*data_variant, max_bucket_of_window))
            final_buckets.emplace(bucket);

    if (final_buckets.empty())
        return {};

    /// Collect finalized windows
    WindowsWithBuckets windows_with_buckets;
    while (*final_buckets.begin() <= max_bucket_of_window)
    {
        auto window_with_buckets = windows_with_buckets.emplace(windows_with_buckets.begin(), WindowWithBuckets{window, {}});
        for (auto time_bucket : final_buckets)
        {
            if (time_bucket >= min_bucket_of_window && time_bucket <= max_bucket_of_window)
                window_with_buckets->buckets.emplace_back(time_bucket);
        }

        if (unlikely(windows_with_buckets.front().buckets.empty()))
            windows_with_buckets.erase(windows_with_buckets.begin());

        /// Previous window
        window.start = addTime(
            window.start, window_params.interval_kind, -window_params.slide_interval, *window_params.time_zone, window_params.time_scale);
        window.end = addTime(
            window.end, window_params.interval_kind, -window_params.slide_interval, *window_params.time_zone, window_params.time_scale);

        calc_window_min_max_buckets();
    }

    return windows_with_buckets;
}

void HopAggregatingTransform::removeBucketsImpl(Int64 watermark)
{
    auto last_expired_time_bucket = HopHelper::getLastExpiredTimeBucket(
        watermark, window_params, params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START);
    params->aggregator.removeBucketsBefore(variants, last_expired_time_bucket);
}

}
}
