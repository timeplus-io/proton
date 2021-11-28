#include "TimestampTransformStep.h"

#include <Processors/Transforms/Streaming/TimestampTransform.h>
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
TimestampTransformStep::TimestampTransformStep(
    const DataStream & input_stream_,
    Block output_header,
    StreamingFunctionDescriptionPtr  timestamp_func_desc_)
    : ITransformingStep(input_stream_, std::move(output_header), getTraits())
    , timestamp_func_desc(std::move(timestamp_func_desc_))
{
}

void TimestampTransformStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & header) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<TimestampTransform>(header, getOutputStream().header, timestamp_func_desc);
    });
}
}
