#pragma once

#include <Interpreters/Streaming/FunctionDescription.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
/// Implement process time filtering like WHERE _tp_time > now() - interval
class ProcessTimeFilterStep final : public ITransformingStep
{
public:
    ProcessTimeFilterStep(const DataStream & input_stream_, BaseScaleInterval interval_bs_, const String & column_name_);

    ~ProcessTimeFilterStep() override = default;

    String getName() const override { return "ProcessTimeFilterStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    BaseScaleInterval interval_bs;
    String column_name;
};
}
}
