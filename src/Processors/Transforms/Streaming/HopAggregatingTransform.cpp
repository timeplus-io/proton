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

WindowsWithBucket HopAggregatingTransform::getFinalizedWindowsWithBucket(Int64 watermark) const
{
    WindowsWithBucket windows_with_bucket;

    auto [last_window_start, last_window_end] = HopHelper::getLastFinalizedWindow(watermark, window_params);
    if (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START)
    {
        std::set<size_t> final_buckets;
        for (const auto & data_variant : many_data->variants)
            for (auto bucket : params->aggregator.bucketsBefore(*data_variant, last_window_start))
                final_buckets.emplace(bucket);

        for (auto time_bucket : final_buckets)
            windows_with_bucket.emplace_back(WindowWithBucket{
                static_cast<Int64>(time_bucket),
                addTime(
                    static_cast<Int64>(time_bucket),
                    window_params.interval_kind,
                    window_params.window_interval,
                    *window_params.time_zone,
                    window_params.time_scale),
                time_bucket});
    }
    else
    {
        std::set<size_t> final_buckets;
        for (const auto & data_variant : many_data->variants)
            for (auto bucket : params->aggregator.bucketsBefore(*data_variant, last_window_end))
                final_buckets.emplace(bucket);

        for (auto time_bucket : final_buckets)
            windows_with_bucket.emplace_back(WindowWithBucket{
                addTime(
                    static_cast<Int64>(time_bucket),
                    window_params.interval_kind,
                    -window_params.window_interval,
                    *window_params.time_zone,
                    window_params.time_scale),
                static_cast<Int64>(time_bucket),
                time_bucket});
    }

    return windows_with_bucket;
}

void HopAggregatingTransform::removeBucketsImpl(Int64 watermark)
{
    auto [last_window_start, last_window_end] = HopHelper::getLastFinalizedWindow(watermark, window_params);
    if (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START)
        params->aggregator.removeBucketsBefore(variants, last_window_start);
    else
        params->aggregator.removeBucketsBefore(variants, last_window_end);
}

}
}
