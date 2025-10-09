#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB::Streaming
{

/// Combine several logical flows and process them sequentially
class DelayStep : public IQueryPlanStep
{
public:
    explicit DelayStep(DataStreams input_streams_);

    String getName() const override { return "Delay"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    Block header;
};

}
