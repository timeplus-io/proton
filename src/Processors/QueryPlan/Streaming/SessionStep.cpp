#include "SessionStep.h"

#include <Processors/Transforms/Streaming/SessionTransform.h>
#include <Processors/Transforms/Streaming/SessionTransformWithSubstream.h>
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

SessionStep::SessionStep(
    const DataStream & input_stream_, Block output_header_, FunctionDescriptionPtr desc_, std::vector<size_t> substream_key_positions_)
    : ITransformingStep(input_stream_, std::move(output_header_), getTraits())
    , desc(std::move(desc_))
    , substream_key_positions(std::move(substream_key_positions_))
{
}

void SessionStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    if (substream_key_positions.empty())
    {
        pipeline.addSimpleTransform([&](const Block & header) {
            return std::make_shared<SessionTransform>(header, output_stream->header, desc);
        });
    }
    else
    {
        pipeline.addSimpleTransform([&](const Block & header) {
            return std::make_shared<SessionTransformWithSubstream>(header, output_stream->header, desc, substream_key_positions);
        });
    }
}
}
}
