#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/Streaming/WatermarkStamper.h>

namespace DB
{
namespace Streaming
{
/// Implement watermark assignment for streaming processing
class WatermarkStep final : public ITransformingStep
{
public:
    WatermarkStep(
        const DataStream & input_stream_, WatermarkStamperParamsPtr params_, bool skip_stamping_for_backfill_data_, Poco::Logger * log);

    ~WatermarkStep() override = default;

    String getName() const override { return "WatermarkStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    WatermarkStamperParamsPtr params;
    bool skip_stamping_for_backfill_data;
    Poco::Logger * log;
};
}
}
