#include <Processors/Transforms/Streaming/HopAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/HopWindowHelper.h>

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

WindowsWithBuckets HopAggregatingTransformWithSubstream::getWindowsWithBuckets(const SubstreamContextPtr & substream_ctx) const
{
    return HopWindowHelper::getWindowsWithBuckets(
        window_params, params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START, [&, this]() {
            return params->aggregator.buckets(substream_ctx->variants);
        });
}

Window HopAggregatingTransformWithSubstream::getLastFinalizedWindow(const SubstreamContextPtr & substream_ctx) const
{
    return HopWindowHelper::getLastFinalizedWindow(substream_ctx->finalized_watermark, window_params);
}

void HopAggregatingTransformWithSubstream::removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx)
{
    auto last_expired_time_bucket = HopWindowHelper::getLastExpiredTimeBucket(
        watermark, window_params, params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START);
    params->aggregator.removeBucketsBefore(substream_ctx->variants, last_expired_time_bucket);
}

}
}
