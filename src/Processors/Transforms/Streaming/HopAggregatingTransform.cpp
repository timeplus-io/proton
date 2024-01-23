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

WindowsWithBuckets HopAggregatingTransform::getLocalWindowsWithBucketsImpl() const
{
    return HopHelper::getWindowsWithBuckets(
        window_params, params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START, [this]() { return getBuckets(); });
}

void HopAggregatingTransform::removeBucketsImpl(Int64 watermark_)
{
    auto last_expired_time_bucket = HopHelper::getLastExpiredTimeBucket(
        watermark_, window_params, params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START);
    params->aggregator.removeBucketsBefore(variants, last_expired_time_bucket);
}

}
}
