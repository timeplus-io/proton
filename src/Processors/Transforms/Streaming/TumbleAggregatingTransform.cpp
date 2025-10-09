#include <Processors/Transforms/Streaming/TumbleAggregatingTransform.h>
#include <Processors/Transforms/Streaming/TumbleWindowHelper.h>

namespace DB
{
namespace Streaming
{
TumbleAggregatingTransform::TumbleAggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const std::string & id)
    : TumbleAggregatingTransform(std::move(header), params_, std::make_unique<ManyAggregatedData>(params_->aggregatorType(), id), 0, 1)
{
}

TumbleAggregatingTransform::TumbleAggregatingTransform(
    Block header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_, size_t current_variant_, size_t max_threads_)
    : WindowAggregatingTransform(
          std::move(header),
          std::move(params_),
          std::move(many_data_),
          current_variant_,
          max_threads_,
          "TumbleAggregatingTransform",
          ProcessorID::TumbleAggregatingTransformID)
    , window_params(params->params->window_params->as<TumbleWindowParams &>())
{
}

WindowsWithBuckets TumbleAggregatingTransform::getLocalWindowsWithBucketsImpl() const
{
    return TumbleWindowHelper::getWindowsWithBuckets(
        window_params, params->params->group_by == IAggregatorParams::GroupBy::WindowStart, [this]() { return getBuckets(); });
}

void TumbleAggregatingTransform::removeBucketsImpl(Int64 watermark_)
{
    auto last_expired_time_bucket = TumbleWindowHelper::getLastExpiredTimeBucket(
        watermark_, window_params, params->params->group_by == IAggregatorParams::GroupBy::WindowStart);

    return params->aggregator->removeBucketsBefore(variants, last_expired_time_bucket, transform_id);
}

String TumbleAggregatingTransform::getName() const
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Memory:
            return "TumbleAggregatingTransform";
        case AggregatorType::Hybrid:
            return "HybridTumbleAggregatingTransform";
    }
}

}

}
