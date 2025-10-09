#include <Processors/Transforms/Streaming/HopAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/HopWindowHelper.h>

namespace DB
{
namespace Streaming
{
HopAggregatingTransformWithSubstream::HopAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_, size_t id)
    : WindowAggregatingTransformWithSubstream(
          std::move(header),
          std::move(params_),
          id,
          "HopAggregatingTransformWithSubstream",
          ProcessorID::HopAggregatingTransformWithSubstreamID)
    , window_params(params->params->window_params->as<HopWindowParams &>())
{
}

WindowsWithBuckets HopAggregatingTransformWithSubstream::getWindowsWithBuckets(const SubstreamAggregatedDataPtr & substream_ctx) const
{
    return HopWindowHelper::getWindowsWithBuckets(
        window_params, params->params->group_by == IAggregatorParams::GroupBy::WindowStart, [&, this]() {
            return params->aggregator->buckets(*substream_ctx->variants);
        });
}

Window HopAggregatingTransformWithSubstream::getLastFinalizedWindow(const SubstreamAggregatedDataPtr & substream_ctx) const
{
    return HopWindowHelper::getLastFinalizedWindow(substream_ctx->finalized_watermark, window_params);
}

void HopAggregatingTransformWithSubstream::removeBucketsImpl(Int64 watermark, const SubstreamAggregatedDataPtr & substream_ctx)
{
    auto last_expired_time_bucket = HopWindowHelper::getLastExpiredTimeBucket(
        watermark, window_params, params->params->group_by == IAggregatorParams::GroupBy::WindowStart);
    return params->aggregator->removeBucketsBefore(*substream_ctx->variants, last_expired_time_bucket, transform_id);
}

String HopAggregatingTransformWithSubstream::getName() const
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Memory:
            return "HopAggregatingTransformWithSubstream";
        case AggregatorType::Hybrid:
            return "HybridHopAggregatingTransformWithSubstream";
    }
}

}
}
