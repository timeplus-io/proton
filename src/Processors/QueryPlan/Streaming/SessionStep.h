#pragma once

#include <Interpreters/Streaming/FunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
/// Implement session transform to evaluate the session start / end predicates
class SessionStep final : public ITransformingStep
{
public:
    SessionStep(
        const DataStream & input_stream_,
        Block output_header,
        FunctionDescriptionPtr desc_,
        std::vector<size_t> substream_key_positions);

    ~SessionStep() override = default;

    String getName() const override { return "SessionStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    FunctionDescriptionPtr desc;
    std::vector<size_t> substream_key_positions;
};
}
}
