#pragma once

#include <Processors/Transforms/Streaming/AggregatingTransformWithSubstream.h>

namespace DB
{
namespace Streaming
{
class WindowAggregatingTransformWithSubstream : public AggregatingTransformWithSubstream
{
public:
    WindowAggregatingTransformWithSubstream(Block header, AggregatingTransformParamsPtr params_, const String & log_name, ProcessorID pid_);

    ~WindowAggregatingTransformWithSubstream() override = default;

protected:
    void finalize(const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx) override;

    void clearExpiredState(Int64 finalized_watermark, const SubstreamContextPtr & substream_ctx) override;

private:
    inline void doFinalize(Int64 watermark, const SubstreamContextPtr & substream_ctx, const ChunkContextPtr & chunk_ctx);

    virtual WindowsWithBuckets getWindowsWithBuckets(const SubstreamContextPtr & substream_ctx) const = 0;
    virtual Window getLastFinalizedWindow(const SubstreamContextPtr & substream_ctx) const = 0;
    virtual void removeBucketsImpl(Int64 watermark, const SubstreamContextPtr & substream_ctx) = 0;
    virtual bool needReassignWindow() const = 0;

    std::optional<size_t> window_start_col_pos;
    std::optional<size_t> window_end_col_pos;

    bool only_emit_finalized_windows = true;
    bool only_convert_updates = false;
};

}
}
