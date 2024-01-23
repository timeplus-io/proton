#pragma once

#include <Processors/Transforms/Streaming/AggregatingTransform.h>

namespace DB
{
namespace Streaming
{
class WindowAggregatingTransform : public AggregatingTransform
{
public:
    WindowAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data_,
        size_t current_variant_,
        size_t max_threads_,
        size_t temporary_data_merge_threads_,
        const String & log_name,
        ProcessorID pid_);

    ~WindowAggregatingTransform() override = default;

protected:
    bool needFinalization(Int64 min_watermark) const override;
    bool prepareFinalization(Int64 min_watermark) override;

    void finalize(const ChunkContextPtr & chunk_ctx) override;

    void clearExpiredState(Int64 finalized_watermark) override;

    std::vector<Int64> getBuckets() const;

private:
    virtual WindowsWithBuckets getLocalWindowsWithBucketsImpl() const = 0;
    virtual void removeBucketsImpl(Int64 watermark) = 0;
    virtual bool needReassignWindow() const = 0;

    /// Prepared windows to finalize in `prepareFinalization`
    WindowsWithBuckets prepared_windows_with_buckets;

    std::optional<size_t> window_start_col_pos;
    std::optional<size_t> window_end_col_pos;

    bool only_emit_finalized_windows = true;
    bool only_convert_updates = false;
};
}
}
