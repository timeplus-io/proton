#pragma once

#include <Interpreters/Streaming/StreamingFunctionDescription.h>
#include <Interpreters/Streaming/StreamingWindowCommon.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
/// Implement process time filtering like WHERE _tp_time > now() - interval
class ProcessTimeFilterStep final : public ITransformingStep
{
public:
    ProcessTimeFilterStep(
        const DataStream & input_stream_, BaseScaleInterval interval_bs_, const String & column_name_);

    ~ProcessTimeFilterStep() override = default;

    String getName() const override { return "ProcessTimeFilterStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    BaseScaleInterval interval_bs;
    String column_name;
};
}
