#include <Processors/QueryPlan/Streaming/ChangelogConvertTransformStep.h>
#include <Processors/Transforms/Streaming/ChangelogConvertTransform.h>
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
        },
        {
            .preserves_number_of_rows = false,
        }};
}
}

ChangelogConvertTransformStep::ChangelogConvertTransformStep(
    const DataStream & input_stream_,
    Block output_header_,
    std::vector<std::string> key_column_names_,
    const std::string & version_column_name_,
    size_t max_thread_)
    : ITransformingStep(input_stream_, ChangelogConvertTransform::transformOutputHeader(output_header_), getTraits())
    , key_column_names(std::move(key_column_names_))
    , version_column_name(version_column_name_)
    , max_thread(max_thread_)
{
}

void ChangelogConvertTransformStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & input_header) -> std::shared_ptr<IProcessor> {
        return std::make_shared<ChangelogConvertTransform>(input_header, getOutputStream().header, key_column_names, version_column_name);
    });
}

}
}
