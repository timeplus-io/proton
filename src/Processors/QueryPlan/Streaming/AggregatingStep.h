#pragma once

#include <Interpreters/Streaming/Aggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

namespace Streaming
{
struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

enum class EmittedAggregatedKind;

/// Streaming Aggregation. See StreamingAggregatingTransform.
class AggregatingStep : public ITransformingStep
{
public:
    AggregatingStep(
        const DataStream & input_stream_,
        Aggregator::Params params_,
        bool final_,
        size_t merge_threads_,
        size_t temporary_data_merge_threads_,
        bool emit_version_,
        bool emit_changelog_,
        WatermarkEmitMode watermark_emit_mode_);

    String getName() const override { return "StreamingAggregating"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const Aggregator::Params & getParams() const { return params; }

private:
    Aggregator::Params params;
    bool final;
    size_t merge_threads;
    size_t temporary_data_merge_threads;

    bool emit_version;
    bool emit_changelog;
    WatermarkEmitMode watermark_emit_mode;

    Processors aggregating;
};
}
}
