#include <Processors/QueryPlan/LightShufflingStep.h>

#include <Processors/Transforms/LightShufflingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
            .preserves_shuffling = false,
        },
        {
            .preserves_number_of_rows = true,
        }};
}

LightShufflingStep::LightShufflingStep(const DataStream & input_stream_, Names keys_, size_t max_num_outputs_)
    : ITransformingStep(input_stream_, input_stream_.header, getTraits())
    , keys(std::move(keys_))
    , max_num_outputs(max_num_outputs_)
{
    output_stream->shuffle_description = ShuffleDescription{ShuffleDescription::Kind::Light, keys};
}

void LightShufflingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto num_input_streams = pipeline.getNumStreams();
    /// auto num_outputs = std::max(num_input_streams, max_num_outputs);
    auto num_outputs = bestTotalOutputStreams(max_num_outputs);

    chassert(num_outputs > 0 && (num_outputs & (num_outputs - 1)) == 0);

    if (num_input_streams == 1 && num_outputs == 1)
    {
        /// 1 -> 1, fast path, no-op
    }
    else
    {
        /// M -> N
        pipeline.addShufflingTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            return std::make_shared<LightShufflingTransform>(header, num_outputs, keys);
        });
    }
}

}
