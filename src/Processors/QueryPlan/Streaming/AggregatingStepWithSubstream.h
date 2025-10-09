#pragma once

#include <Interpreters/Streaming/Aggregator/IAggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB::Streaming
{

struct EmitParams;

class AggregatingStepWithSubstream final : public ITransformingStep
{
public:
    AggregatingStepWithSubstream(
        const DataStream & input_stream_,
        IAggregatorParamsPtr params_,
        std::shared_ptr<const Streaming::EmitParams> emit_params_,
        bool emit_version_,
        bool emit_changelog_,
        bool aggregation_backfill_key_unique_);

    String getName() const override { return "AggregatingWithSubstream"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const IAggregatorParams & getParams() const { return *params; }

private:
    void updateOutputStream() override;

    IAggregatorParamsPtr params;
    std::shared_ptr<const Streaming::EmitParams> emit_params;
    bool emit_version;
    bool emit_changelog;
    bool aggregation_backfill_key_unique;

    Processors aggregating;
};

}
