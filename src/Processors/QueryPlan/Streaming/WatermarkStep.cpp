#include "WatermarkStep.h"

#include <Processors/Transforms/Streaming/WatermarkTransform.h>
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
WatermarkStep::WatermarkStep(
    const DataStream & input_stream_,
    Block output_header_,
    ASTPtr query_,
    TreeRewriterResultPtr syntax_analyzer_result_,
    FunctionDescriptionPtr desc_,
    bool proc_time_,
    Poco::Logger * log_)
    : ITransformingStep(input_stream_, std::move(output_header_), getTraits())
    , query(std::move(query_))
    , syntax_analyzer_result(std::move(syntax_analyzer_result_))
    , desc(std::move(desc_))
    , proc_time(proc_time_)
    , log(log_)
{
}

void WatermarkStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & header) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<WatermarkTransform>(query, syntax_analyzer_result, desc, proc_time, header, output_stream->header, log);
    });
}
}
}
