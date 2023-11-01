#include <Processors/QueryPlan/Streaming/ReplayStreamStep.h>
#include <Processors/Streaming/ReplayStreamTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{
namespace Streaming
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

ReplayStreamStep::ReplayStreamStep(const DataStream & input_stream_, Float32 replay_speed_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits()), replay_speed(replay_speed_)
{
}

void ReplayStreamStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header) { return std::make_shared<ReplayStreamTransform>(header, replay_speed); });
}

}
}
