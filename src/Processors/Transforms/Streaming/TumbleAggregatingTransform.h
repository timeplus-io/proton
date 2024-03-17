#pragma once

#include <Processors/Transforms/Streaming/WindowAggregatingTransform.h>

namespace DB
{
namespace Streaming
{
class TumbleAggregatingTransform final : public WindowAggregatingTransform
{
public:
    TumbleAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    TumbleAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data_,
        size_t current_variant_,
        size_t max_threads_,
        size_t temporary_data_merge_threads_);

    ~TumbleAggregatingTransform() override = default;

    String getName() const override { return "TumbleAggregatingTransform"; }

private:
    WindowsWithBuckets getLocalWindowsWithBucketsImpl() const override;
    void removeBucketsImpl(Int64 watermark) override;
    bool needReassignWindow() const override { return false; }

private:
    TumbleWindowParams & window_params;
};
}
}
