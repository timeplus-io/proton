#pragma once

#include <Processors/Transforms/Streaming/WindowAggregatingTransform.h>

namespace DB
{
namespace Streaming
{
class HopAggregatingTransform final : public WindowAggregatingTransform
{
public:
    HopAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    HopAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data_,
        size_t current_variant_,
        size_t max_threads_,
        size_t temporary_data_merge_threads_);

    ~HopAggregatingTransform() override = default;

    String getName() const override { return "HopAggregatingTransform"; }

private:
    WindowsWithBuckets getLocalWindowsWithBucketsImpl() const override;
    void removeBucketsImpl(Int64 watermark) override;
    bool needReassignWindow() const override { return true; }

private:
    HopWindowParams & window_params;
};
}
}
