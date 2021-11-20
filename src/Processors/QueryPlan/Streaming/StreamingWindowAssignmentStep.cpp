#include "StreamingWindowAssignmentStep.h"

#include <Processors/Transforms/Streaming/StreamingWindowAssignmentTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

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

namespace DB
{
StreamingWindowAssignmentStep::StreamingWindowAssignmentStep(
    const DataStream & input_stream_,
    const Block & output_header,
    StreamingFunctionDescriptionPtr desc_)
    : ITransformingStep(input_stream_, output_header, getTraits())
    , desc(desc_)
{
}

void StreamingWindowAssignmentStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & header) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<StreamingWindowAssignmentTransform>(header, getOutputStream().header, desc);
    });
}
}
