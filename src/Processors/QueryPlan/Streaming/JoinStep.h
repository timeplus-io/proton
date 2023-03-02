#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

namespace Streaming
{
/// Join two data streams.
class JoinStep final : public IQueryPlanStep
{
public:
    JoinStep(
        const DataStream & left_stream_,
        const DataStream & right_stream_,
        JoinPtr join_,
        size_t max_block_size_,
        size_t join_max_cached_bytes,
        size_t max_streams);

    String getName() const override { return "StreamingJoin"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    const JoinPtr & getJoin() const { return join; }

private:
    JoinPtr join;
    size_t max_block_size;
    size_t join_max_cached_bytes;
    size_t max_streams;
    Processors processors;
};
}
}
