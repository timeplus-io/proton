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

private:
    void finalize(const ChunkContextPtr & chunk_ctx) override;

    inline void doFinalize(Int64 watermark, const ChunkContextPtr & chunk_ctx);

    inline void initialize(ManyAggregatedDataVariantsPtr & data);

    void convertTwoLevel(ManyAggregatedDataVariantsPtr & data, Int64 watermark, const ChunkContextPtr & chunk_ctx);

    virtual WindowsWithBucket getFinalizedWindowsWithBucket(Int64 watermark) const = 0;
    virtual void removeBucketsImpl(Int64 watermark) = 0;
    virtual bool needReassignWindow() const = 0;
};
}
}
