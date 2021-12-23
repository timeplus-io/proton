#include "ProcessTimeFilterStep.h"

#include <Processors/Transforms/Streaming/ProcessTimeFilter.h>
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
ProcessTimeFilterStep::ProcessTimeFilterStep(
    const DataStream & input_stream_,
    Int64 interval_seconds_,
    const String & column_name_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , interval_seconds(interval_seconds_)
    , column_name(column_name_)
{
}

void ProcessTimeFilterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & header) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<ProcessTimeFilter>(column_name, interval_seconds, header);
    });
}
}
