#include "WatermarkStepWithSubstream.h"

#include <Processors/Transforms/Streaming/WatermarkTransformWithSubstream.h>
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

WatermarkStepWithSubstream::WatermarkStepWithSubstream(
    const DataStream & input_stream_,
    WatermarkStamperParams params_,
    Poco::Logger * log_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , params(std::move(params_))
    , log(log_)
{
}

void WatermarkStepWithSubstream::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /* settings */)
{
    pipeline.addSimpleTransform([&](const Block & header) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        return std::make_shared<WatermarkTransformWithSubstream>(header, std::move(params), log);
    });
}
}
}
