#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{
namespace Streaming
{
/// M shards -> N virtual shards (threads) shuffling
class ShufflingStep final : public ITransformingStep
{
public:
    ShufflingStep(const DataStream & input_stream_, std::vector<size_t> key_positions_, size_t max_thread_);

    String getName() const override { return "ShufflingStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    std::vector<size_t> key_positions;
    size_t max_thread;
};

}
}
