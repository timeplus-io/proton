#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
/// #include <QueryPipeline/SizeLimits.h>

namespace DB
{
/// M shards -> N virtual shards (threads) shuffling without calculating the substream ID when compared with Streaming::ShufflingStep
class LightShufflingStep final : public ITransformingStep
{
public:
    LightShufflingStep(const DataStream & input_stream_, std::vector<size_t> key_positions_, size_t max_num_outputs_);

    String getName() const override { return "LightShufflingStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    std::vector<size_t> key_positions;
    size_t max_num_outputs;
};

}
