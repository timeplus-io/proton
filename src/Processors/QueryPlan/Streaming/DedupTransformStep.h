#pragma once

#include <Interpreters/Streaming/StreamingFunctionDescription.h>
/// #include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
class DedupTransformStep final : public ITransformingStep
{
public:
    DedupTransformStep(
        const DataStream & input_stream_,
        Block output_header,
        StreamingFunctionDescriptionPtr  dedup_func_desc_);

    ~DedupTransformStep() override = default;

    String getName() const override { return "DedupTransformStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    StreamingFunctionDescriptionPtr dedup_func_desc;
};
}
