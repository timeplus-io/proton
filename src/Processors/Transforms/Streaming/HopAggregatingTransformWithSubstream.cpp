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

WindowsWithBuckets
HopAggregatingTransformWithSubstream::getFinalizedWindowsWithBuckets(Int64 watermark, const SubstreamContextPtr & substream_ctx) const
{
    return HopHelper::getFinalizedWindowsWithBuckets(
        watermark, window_params, params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START, [&, this](Int64 max_bucket) {
            return params->aggregator.bucketsBefore(substream_ctx->variants, max_bucket);
        });
}

void HopAggregatingTransformWithSubstream::removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx)
{
    auto last_expired_time_bucket = HopHelper::getLastExpiredTimeBucket(
        watermark, window_params, params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START);
    params->aggregator.removeBucketsBefore(substream_ctx->variants, last_expired_time_bucket);
}

}
}
