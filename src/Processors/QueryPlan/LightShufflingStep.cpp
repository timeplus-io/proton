#include <Processors/QueryPlan/LightShufflingStep.h>

#include <Processors/Transforms/LightShufflingTransform.h>
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

LightShufflingStep::LightShufflingStep(const DataStream & input_stream_, std::vector<size_t> key_positions_, size_t max_num_outputs_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , key_positions(std::move(key_positions_))
    , max_num_outputs(max_num_outputs_)
{
}

void LightShufflingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto num_input_streams = pipeline.getNumStreams();
    auto num_outputs = std::max(num_input_streams, max_num_outputs);
    num_outputs = bestTotalOutputStreams(num_outputs);

    assert(num_outputs > 0 && (num_outputs & (num_outputs - 1)) == 0);

    if (num_input_streams == 1 && num_outputs == 1)
    {
        /// 1 -> 1, fast path, no-op
    }
    else
    {
        /// M -> N
        pipeline.addLightShufflingTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            return std::make_shared<LightShufflingTransform>(header, num_outputs, key_positions);
        });
    }
}

}
}
