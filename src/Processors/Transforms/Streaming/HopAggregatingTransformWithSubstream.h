#pragma once

#include <Processors/Transforms/Streaming/WindowAggregatingTransformWithSubstream.h>

namespace DB
{
namespace Streaming
{
class HopAggregatingTransformWithSubstream final : public WindowAggregatingTransformWithSubstream
{
public:
    HopAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_, size_t id);

    ~HopAggregatingTransformWithSubstream() override = default;

    String getName() const override;

private:
    WindowsWithBuckets getWindowsWithBuckets(const SubstreamAggregatedDataPtr & substream_ctx) const override;
    Window getLastFinalizedWindow(const SubstreamAggregatedDataPtr & substream_ctx) const override;
    void removeBucketsImpl(Int64 watermark, const SubstreamAggregatedDataPtr & substream_ctx) override;
    bool needReassignWindow() const override { return true; }

private:
    HopWindowParams & window_params;
};
}
}
