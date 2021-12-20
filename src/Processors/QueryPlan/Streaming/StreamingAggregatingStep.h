#pragma once

#include <Interpreters/Streaming/StreamingAggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

struct StreamingAggregatingTransformParams;
using StreamingAggregatingTransformParamsPtr = std::shared_ptr<StreamingAggregatingTransformParams>;

/// Streaming Aggregation. See StreamingAggregatingTransform.
class StreamingAggregatingStep : public ITransformingStep
{
public:
    StreamingAggregatingStep(
        const DataStream & input_stream_,
        StreamingAggregator::Params params_,
        bool final_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool storage_has_evenly_distributed_read_);

    String getName() const override { return "StreamingAggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const StreamingAggregator::Params & getParams() const { return params; }

private:
    StreamingAggregator::Params params;
    bool final;
    size_t merge_threads;
    size_t temporary_data_merge_threads;

    bool storage_has_evenly_distributed_read;

    Processors aggregating;
};

}
