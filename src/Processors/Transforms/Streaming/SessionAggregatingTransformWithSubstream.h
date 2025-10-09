#pragma once

#include <Processors/Transforms/Streaming/WindowAggregatingTransformWithSubstream.h>

namespace DB
{
namespace Streaming
{
class SessionAggregatingTransformWithSubstream final : public WindowAggregatingTransformWithSubstream
{
public:
    SessionAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_, size_t id);

    ~SessionAggregatingTransformWithSubstream() override = default;

    String getName() const override;

private:
    void initSubstreamContext(const SubstreamAggregatedDataPtr & substream_ctx) override;
    
    std::pair<bool, bool> executeOrMergeColumns(Chunk & chunk, const SubstreamAggregatedDataPtr & substream_ctx) override;
    WindowsWithBuckets getWindowsWithBuckets(const SubstreamAggregatedDataPtr & substream_ctx) const override;
    Window getLastFinalizedWindow(const SubstreamAggregatedDataPtr & substream_ctx) const override;
    void removeBucketsImpl(Int64 watermark, const SubstreamAggregatedDataPtr & substream_ctx) override;
    bool needReassignWindow() const override { return true; }

private:
    SessionWindowParams & window_params;

    ssize_t wstart_col_pos = -1;
    ssize_t wend_col_pos = -1;
    size_t time_col_pos;
    size_t session_start_col_pos;
    size_t session_end_col_pos;
};
}
}
