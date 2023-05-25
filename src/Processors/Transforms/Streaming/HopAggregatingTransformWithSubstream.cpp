#include <Processors/Transforms/Streaming/HopAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/HopHelper.h>

namespace DB
{
namespace Streaming
{
HopAggregatingTransformWithSubstream::HopAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_)
    : WindowAggregatingTransformWithSubstream(
        std::move(header), std::move(params_), "HopAggregatingTransformWithSubstream", ProcessorID::HopAggregatingTransformWithSubstreamID)
    , window_params(params->params.window_params->as<HopWindowParams &>())
{
}

WindowsWithBucket HopAggregatingTransformWithSubstream::getFinalizedWindowsWithBucket(
    Int64 watermark, const SubstreamContextPtr & substream_ctx) const
{
    WindowsWithBucket windows_with_bucket;

    auto [last_window_start, last_window_end] = HopHelper::getLastFinalizedWindow(watermark, window_params);
    if (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START)
    {
        const auto & final_buckets = params->aggregator.bucketsBefore(substream_ctx->variants, last_window_start);
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
        const auto & final_buckets = params->aggregator.bucketsBefore(substream_ctx->variants, last_window_end);
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

void HopAggregatingTransformWithSubstream::removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx)
{
    auto [last_window_start, last_window_end] = HopHelper::getLastFinalizedWindow(watermark, window_params);
    if (params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START)
        params->aggregator.removeBucketsBefore(substream_ctx->variants, last_window_start);
    else
        params->aggregator.removeBucketsBefore(substream_ctx->variants, last_window_end);
}

}
}
