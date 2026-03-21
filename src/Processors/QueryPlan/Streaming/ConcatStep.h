#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB::Streaming
{

/// Concat several logical streams of data into single logical stream with specified structure.
/// Only used for backfill concat
class ConcatStep : public IQueryPlanStep
{
public:
    explicit ConcatStep(DataStreams input_streams_, bool disable_concat_callback_, UInt64 backfill_max_threads_);

    String getName() const override { return "Concat"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    Block header;
    bool disable_concat_callback;
    UInt64 backfill_max_threads;
};

}
