#include <Processors/Transforms/Streaming/TumbleAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/TumbleWindowHelper.h>

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

WindowsWithBuckets TumbleAggregatingTransformWithSubstream::getWindowsWithBuckets(const SubstreamContextPtr & substream_ctx) const
{
    return TumbleWindowHelper::getWindowsWithBuckets(
        window_params, params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START, [&, this]() {
            return params->aggregator.buckets(substream_ctx->variants);
        });
}

Window TumbleAggregatingTransformWithSubstream::getLastFinalizedWindow(const SubstreamContextPtr & substream_ctx) const
{
    return TumbleWindowHelper::getLastFinalizedWindow(substream_ctx->finalized_watermark, window_params);
}

void TumbleAggregatingTransformWithSubstream::removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx)
{
    auto last_expired_time_bucket = TumbleWindowHelper::getLastExpiredTimeBucket(
        watermark, window_params, params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START);

    params->aggregator.removeBucketsBefore(substream_ctx->variants, last_expired_time_bucket);
}

}
}
