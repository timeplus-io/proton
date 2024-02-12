#pragma once

#include <Processors/Transforms/Streaming/WindowAggregatingTransformWithSubstream.h>

namespace DB
{
namespace Streaming
{
class TumbleAggregatingTransformWithSubstream final : public WindowAggregatingTransformWithSubstream
{
public:
    TumbleAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_);

    ~TumbleAggregatingTransformWithSubstream() override = default;

    String getName() const override { return "TumbleAggregatingTransformWithSubstream"; }

private:
    WindowsWithBuckets getWindowsWithBuckets(const SubstreamContextPtr & substream_ctx) const override;
    Window getLastFinalizedWindow(const SubstreamContextPtr & substream_ctx) const override;
    void removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx) override;
    bool needReassignWindow() const override { return false; }

private:
    TumbleWindowParams & window_params;
};
}
}
