#include <Processors/Transforms/Streaming/TumbleAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/TumbleWindowHelper.h>

namespace DB
{
namespace Streaming
{
TumbleAggregatingTransformWithSubstream::TumbleAggregatingTransformWithSubstream(
    Block header, AggregatingTransformParamsPtr params_, size_t id)
    : WindowAggregatingTransformWithSubstream(
          std::move(header),
          std::move(params_),
          id,
          "TumbleAggregatingTransformWithSubstream",
          ProcessorID::TumbleAggregatingTransformWithSubstreamID)
    , window_params(params->params->window_params->as<TumbleWindowParams &>())
{
}

WindowsWithBuckets TumbleAggregatingTransformWithSubstream::getWindowsWithBuckets(const SubstreamAggregatedDataPtr & substream_ctx) const
{
    return TumbleWindowHelper::getWindowsWithBuckets(
        window_params, params->params->group_by == IAggregatorParams::GroupBy::WindowStart, [&, this]() {
            return params->aggregator->buckets(*substream_ctx->variants);
        });
}

Window TumbleAggregatingTransformWithSubstream::getLastFinalizedWindow(const SubstreamAggregatedDataPtr & substream_ctx) const
{
    return TumbleWindowHelper::getLastFinalizedWindow(substream_ctx->finalized_watermark, window_params);
}

void TumbleAggregatingTransformWithSubstream::removeBucketsImpl(Int64 watermark, const SubstreamAggregatedDataPtr & substream_ctx)
{
    auto last_expired_time_bucket = TumbleWindowHelper::getLastExpiredTimeBucket(
        watermark, window_params, params->params->group_by == IAggregatorParams::GroupBy::WindowStart);

    params->aggregator->removeBucketsBefore(*substream_ctx->variants, last_expired_time_bucket, transform_id);
}

String TumbleAggregatingTransformWithSubstream::getName() const
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Memory:
            return "TumbleAggregatingTransformWithSubstream";
        case AggregatorType::Hybrid:
            return "HybridTumbleAggregatingTransformWithSubstream";
    }
}

}

}
