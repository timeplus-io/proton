#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB::Streaming
{
class RowifyTransformStep final : public ITransformingStep
{
public:
    RowifyTransformStep(const DataStream & input_stream_);

    ~RowifyTransformStep() override = default;

    String getName() const override { return "RowifyTransformStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    void updateOutputStream() override;
};

}
