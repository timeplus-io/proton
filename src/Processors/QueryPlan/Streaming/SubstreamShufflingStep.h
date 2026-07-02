#pragma once

#include <Core/Names.h>
#include <Processors/QueryPlan/ITransformingStep.h>
/// #include <QueryPipeline/SizeLimits.h>

namespace DB
{
namespace Streaming
{
/// M shards -> N virtual shards (threads) shuffling
class SubstreamShufflingStep final : public ITransformingStep
{
public:
    SubstreamShufflingStep(const DataStream & input_stream_, Names keys_, size_t max_thread_);

    String getName() const override { return "SubstreamShufflingStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override;

    Names keys;
    size_t max_thread;
};

}
}
