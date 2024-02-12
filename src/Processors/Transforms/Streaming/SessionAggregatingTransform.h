#pragma once

#include <Processors/Transforms/Streaming/WindowAggregatingTransform.h>

namespace DB
{
namespace Streaming
{
class SessionAggregatingTransform final : public WindowAggregatingTransform
{
public:
    SessionAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    ~SessionAggregatingTransform() override = default;

    String getName() const override { return "SessionAggregatingTransform"; }

private:
    std::pair<bool, bool> executeOrMergeColumns(Chunk & chunk, size_t num_rows) override;
    WindowsWithBuckets getLocalWindowsWithBucketsImpl() const override;
    void removeBucketsImpl(Int64 watermark) override;
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
