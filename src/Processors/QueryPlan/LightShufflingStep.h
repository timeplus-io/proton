#pragma once

#include <Core/Names.h>
#include <Processors/QueryPlan/ITransformingStep.h>
/// #include <QueryPipeline/SizeLimits.h>

namespace DB
{
/// M shards -> N virtual shards (threads) shuffling without calculating the substream ID when compared with Streaming::ShufflingStep
class LightShufflingStep final : public ITransformingStep
{
public:
    LightShufflingStep(const DataStream & input_stream_, Names keys_, size_t max_num_outputs_);

    String getName() const override { return "LightShufflingStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override
    {
        output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
        output_stream->shuffle_description = ShuffleDescription{ShuffleDescription::Kind::Light, keys};
    }

    Names keys;
    size_t max_num_outputs;
};

}
