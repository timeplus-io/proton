#include "WindowAssignmentStep.h"

#include <Processors/Transforms/Streaming/WindowAssignmentTransform.h>
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

WindowAssignmentStep::WindowAssignmentStep(
    const DataStream & input_stream_,
    Block output_header,
    FunctionDescriptionPtr desc_)
    : ITransformingStep(input_stream_, std::move(output_header), getTraits())
    , desc(std::move(desc_))
{
}

void WindowAssignmentStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & header) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<WindowAssignmentTransform>(header, getOutputStream().header, desc);
    });
}
}
}

