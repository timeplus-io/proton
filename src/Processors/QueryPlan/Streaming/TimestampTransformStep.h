#pragma once

#include <Interpreters/Streaming/TimestampFunctionDescription.h>
/// #include <Interpreters/TreeRewriter.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
namespace Streaming
{
class TimestampTransformStep final : public ITransformingStep
{
public:
    TimestampTransformStep(
        const DataStream & input_stream_, Block output_header, TimestampFunctionDescriptionPtr timestamp_func_desc_, bool backfill_);

    ~TimestampTransformStep() override = default;

    String getName() const override { return "TimestampTransformStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    TimestampFunctionDescriptionPtr timestamp_func_desc;
    bool backfill;
};
}
}
