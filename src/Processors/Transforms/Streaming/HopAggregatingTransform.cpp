#include <Processors/Transforms/Streaming/HopAggregatingTransform.h>
#include <Processors/Transforms/Streaming/HopWindowHelper.h>

namespace DB
{
namespace Streaming
{
HopAggregatingTransform::HopAggregatingTransform(Block header, AggregatingTransformParamsPtr params_, const std::string & id)
    : HopAggregatingTransform(std::move(header), params_, std::make_unique<ManyAggregatedData>(params_->aggregatorType(), id), 0, 1)
{
}

HopAggregatingTransform::HopAggregatingTransform(
    Block header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_, size_t current_variant_, size_t max_threads_)
    : WindowAggregatingTransform(
          std::move(header),
          std::move(params_),
          std::move(many_data_),
          current_variant_,
          max_threads_,
          "HopAggregatingTransform",
          ProcessorID::HopAggregatingTransformID)
    , window_params(params->params->window_params->as<HopWindowParams &>())
{
}

WindowsWithBuckets HopAggregatingTransform::getLocalWindowsWithBucketsImpl() const
{
    return HopWindowHelper::getWindowsWithBuckets(
        window_params, params->params->group_by == IAggregatorParams::GroupBy::WindowStart, [this]() { return getBuckets(); });
}

void HopAggregatingTransform::removeBucketsImpl(Int64 watermark_)
{
    auto last_expired_time_bucket = HopWindowHelper::getLastExpiredTimeBucket(
        watermark_, window_params, params->params->group_by == IAggregatorParams::GroupBy::WindowStart);
    return params->aggregator->removeBucketsBefore(variants, last_expired_time_bucket, transform_id);
}

String HopAggregatingTransform::getName() const
{
    switch (params->aggregatorType())
    {
        case AggregatorType::Memory:
            return "HopAggregatingTransform";
        case AggregatorType::Hybrid:
            return "HybridHopAggregatingTransform";
    }
}

}

}
