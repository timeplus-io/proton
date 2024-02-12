#include <Processors/QueryPlan/Streaming/WindowAssignmentStep.h>

#include <Processors/Transforms/Streaming/HopWindowAssignmentTransform.h>
#include <Processors/Transforms/Streaming/SessionWindowAssignmentTransform.h>
#include <Processors/Transforms/Streaming/TumbleWindowAssignmentTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace Streaming
{
namespace
{
DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }};
}
}

WindowAssignmentStep::WindowAssignmentStep(const DataStream & input_stream_, Block output_header, WindowParamsPtr window_params_)
    : ITransformingStep(input_stream_, std::move(output_header), getTraits()), window_params(std::move(window_params_))
{
}

void WindowAssignmentStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    assert(window_params);

    pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
        if (window_params->type == WindowType::Tumble)
            return std::make_shared<TumbleWindowAssignmentTransform>(header, getOutputStream().header, window_params);
        else if (window_params->type == WindowType::Hop)
            return std::make_shared<HopWindowAssignmentTransform>(header, getOutputStream().header, window_params);
        else if (window_params->type == WindowType::Session)
            return std::make_shared<SessionWindowAssignmentTransform>(header, getOutputStream().header, window_params);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "No support window type: {}", magic_enum::enum_name(window_params->type));
    });
}
}
}
