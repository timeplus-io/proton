#include "ShufflingStep.h"

#include <Processors/Transforms/Streaming/ShufflingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace Streaming
{
static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = true,
        }};
}

ShufflingStep::ShufflingStep(const DataStream & input_stream_, std::vector<size_t> key_positions_, size_t max_thread_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits()), key_positions(std::move(key_positions_)), max_thread(max_thread_)
{
}

void ShufflingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// FIXME: @output_num - more figured automatically
    size_t output_num = max_thread;
    pipeline.addTransform(std::make_shared<ShufflingTransform>(pipeline.getHeader(), pipeline.getNumStreams(), output_num, key_positions));
}

}
}
