#pragma once

#include <Interpreters/Streaming/WindowCommon.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
/// Implement watermark assignment for streaming processing
class WindowAssignmentStep final : public ITransformingStep
{
public:
    WindowAssignmentStep(const DataStream & input_stream_, Block output_header, WindowParamsPtr window_params_);

    ~WindowAssignmentStep() override = default;

    String getName() const override { return "StreamingWindowAssignmentStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    WindowParamsPtr window_params;
};
}
}
