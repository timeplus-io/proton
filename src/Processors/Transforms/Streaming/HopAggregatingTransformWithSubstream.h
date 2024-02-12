#pragma once

#include <Processors/Transforms/Streaming/WindowAggregatingTransformWithSubstream.h>

namespace DB
{
namespace Streaming
{
class HopAggregatingTransformWithSubstream final : public WindowAggregatingTransformWithSubstream
{
public:
    HopAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_);

    ~HopAggregatingTransformWithSubstream() override = default;

    String getName() const override { return "HopAggregatingTransformWithSubstream"; }

private:
    WindowsWithBuckets getWindowsWithBuckets(const SubstreamContextPtr & substream_ctx) const override;
    Window getLastFinalizedWindow(const SubstreamContextPtr & substream_ctx) const override;
    void removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx) override;
    bool needReassignWindow() const override { return true; }

private:
    HopWindowParams & window_params;
};
}
}
