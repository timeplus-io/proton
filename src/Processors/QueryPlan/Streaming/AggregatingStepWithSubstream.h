#pragma once

#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

namespace Streaming
{
struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

class AggregatingStepWithSubstream final : public ITransformingStep
{
public:
    AggregatingStepWithSubstream(
        const DataStream & input_stream_,
        Aggregator::Params params_,
        bool final_,
        bool emit_version_,
        bool emit_changelog_,
        EmitMode emit_mode_);

    String getName() const override { return "StreamingAggregatingWithSubstream"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const Aggregator::Params & getParams() const { return params; }

private:
    Aggregator::Params params;
    bool final;
    bool emit_version;
    bool emit_changelog;
    EmitMode emit_mode;

    Processors aggregating;
};
}
}
