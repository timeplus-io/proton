#include "SessionStepWithSubstream.h"

#include <Processors/Transforms/Streaming/SessionTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/Sessionizer.h>
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
            .preserves_number_of_rows = false,
        }};
}
}

SessionStepWithSubstream::SessionStepWithSubstream(const DataStream & input_stream_, Block output_header_, FunctionDescriptionPtr desc_)
    : ITransformingStep(input_stream_, std::move(output_header_), getTraits()), desc(std::move(desc_))
{
}

void SessionStepWithSubstream::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform(
        [&](const Block & header) { return std::make_shared<SessionTransformWithSubstream>(header, output_stream->header, desc); });
}
}
}
