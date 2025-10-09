#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/Streaming/EmitParams.h>

namespace DB
{
namespace Streaming
{
/// Implement watermark assignment for streaming processing
class WatermarkStep final : public ITransformingStep
{
public:
    WatermarkStep(const DataStream & input_stream_, EmitParamsPtr params_, bool skip_stamping_for_backfill_data_);

    ~WatermarkStep() override = default;

    String getName() const override { return "WatermarkStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    void updateOutputStream() override
    {
        output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
    }

    EmitParamsPtr params;
    bool skip_stamping_for_backfill_data;
};
}
}
