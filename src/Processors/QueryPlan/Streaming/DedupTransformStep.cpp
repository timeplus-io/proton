#include "DedupTransformStep.h"

#include <Processors/Transforms/Streaming/DedupTransform.h>
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

DedupTransformStep::DedupTransformStep(
    const DataStream & input_stream_,
    Block output_header,
    TableFunctionDescriptionPtr dedup_func_desc_)
    : ITransformingStep(input_stream_, std::move(output_header), getTraits())
    , dedup_func_desc(std::move(dedup_func_desc_))
{
}

void DedupTransformStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    /// Use one dedup transform for all input streams
    pipeline.resize(1);
    pipeline.addSimpleTransform([&](const Block & header) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<DedupTransform>(header, getOutputStream().header, dedup_func_desc);
    });
}
}
}

