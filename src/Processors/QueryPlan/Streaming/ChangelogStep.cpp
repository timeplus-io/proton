#include <Processors/QueryPlan/Streaming/ChangelogStep.h>
#include <Processors/Transforms/Streaming/ChangelogTransform.h>
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
            .preserves_sorting = false,
            .preserves_substream = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}
}

ChangelogStep::ChangelogStep(
    const DataStream & input_stream_,
    Block output_header_,
    std::vector<std::string> key_column_names_,
    const std::string & version_column_name_)
    : ITransformingStep(input_stream_, output_header_, getTraits())
    , key_column_names(std::move(key_column_names_))
    , version_column_name(version_column_name_)
{
}

void ChangelogStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & input_header) -> std::shared_ptr<IProcessor> {
        return std::make_shared<ChangelogTransform>(input_header, getOutputStream().header, key_column_names, version_column_name);
    });
}

void ChangelogStep::updateOutputStream()
{
    auto & output_header = output_stream->header;
    output_stream = createOutputStream(
        input_streams.front(), std::move(output_header), getDataStreamTraits());
}
}
}
