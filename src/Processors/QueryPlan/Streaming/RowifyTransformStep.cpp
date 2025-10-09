#include <Processors/QueryPlan/Streaming/RowifyTransformStep.h>

#include <Processors/Transforms/Streaming/RowifyTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB::Streaming
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
            .preserves_substream = false,
        },
        {
            .preserves_number_of_rows = true,
        }};
}
}

RowifyTransformStep::RowifyTransformStep(const DataStream & input_stream_) : ITransformingStep(input_stream_, input_stream_.header, getTraits())
{
}

void RowifyTransformStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<RowifyTransform>(header);
    });
}

void RowifyTransformStep::updateOutputStream()
{
    auto & output_header = output_stream->header;
    output_stream = createOutputStream(input_streams.front(), std::move(output_header), getDataStreamTraits());
}

}
