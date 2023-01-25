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
    /// We like to limit the number of of the outputs
    /// 1) No more than number of inputs concurrency
    /// Depending on aggregation, more output stream may hurt perf
    /// so we just limit the number of output stream here
    auto output_num = std::min(pipeline.getNumStreams(), max_thread);
    assert(output_num >= 1);

    if (pipeline.getNumStreams() > 1)
    {
        /// M -> N
        pipeline.addShufflingTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            return std::make_shared<ShufflingTransform>(pipeline.getHeader(), output_num, key_positions);
        });
    }
    else
    {
        /// 1 -> 1
        pipeline.addTransform(std::make_shared<ShufflingTransform>(pipeline.getHeader(), output_num, key_positions));
    }
}

}
}
