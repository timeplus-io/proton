#pragma once

#include <Interpreters/StreamingFunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
/// Implement watermark assignment for streaming processing
class StreamingWindowAssignmentStep final : public ITransformingStep
{
public:
    StreamingWindowAssignmentStep(
        const DataStream & input_stream_,
        const Names & column_names_,
        StreamingFunctionDescriptionPtr desc_,
        ContextPtr context_);

    ~StreamingWindowAssignmentStep() override = default;

    String getName() const override { return "StreamingWindowAssignmentStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    Names column_names;
    StreamingFunctionDescriptionPtr desc;
    ContextPtr context;
};
}
