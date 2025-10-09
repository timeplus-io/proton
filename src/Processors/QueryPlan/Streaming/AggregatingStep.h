#pragma once

#include <Core/Streaming/Watermark.h>
#include <Interpreters/Streaming/Aggregator/IAggregator.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB::Streaming
{

struct EmitParams;

/// Streaming Aggregation. See StreamingAggregatingTransform.
class AggregatingStep final : public ITransformingStep
{
public:
    AggregatingStep(
        const DataStream & input_stream_,
        IAggregatorParamsPtr params_,
        std::shared_ptr<const Streaming::EmitParams> watermark_params_,
        size_t merge_threads_,
        bool emit_version_,
        bool emit_changelog_,
        bool keys_already_sharded_,
        bool aggregation_backfill_key_unique_);

    String getName() const override;

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void describeActions(FormatSettings &) const override;
    void describePipeline(FormatSettings & settings) const override;

    const IAggregatorParams & getParams() const { return *params; }

private:
    void updateOutputStream() override;

    IAggregatorParamsPtr params;
    std::shared_ptr<const Streaming::EmitParams> emit_params;

    size_t merge_threads;

    bool emit_version;
    bool emit_changelog;
    bool keys_already_sharded;
    bool aggregation_backfill_key_unique;

    Processors aggregating;
};

}
