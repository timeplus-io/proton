#include "WatermarkStep.h"

#include <Processors/Transforms/Streaming/WatermarkTransform.h>
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
WatermarkStep::WatermarkStep(
    const DataStream & input_stream_,
    ASTPtr query_,
    TreeRewriterResultPtr syntax_analyzer_result_,
    StreamingFunctionDescriptionPtr desc_,
    Poco::Logger * log_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , query(query_)
    , syntax_analyzer_result(syntax_analyzer_result_)
    , desc(desc_)
    , log(log_)
{
}

void WatermarkStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & header) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<WatermarkTransform>(query, syntax_analyzer_result, desc, header, log);
    });
}
}
