#pragma once

#include <Interpreters/Streaming/FunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
/// Implement session transform to evaluate the session start / end predicates
class SessionStepWithSubstream final : public ITransformingStep
{
public:
    SessionStepWithSubstream(const DataStream & input_stream_, Block output_header, FunctionDescriptionPtr desc_);

    ~SessionStepWithSubstream() override = default;

    String getName() const override { return "SessionStepWithSubstream"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    FunctionDescriptionPtr desc;
};
}
}
