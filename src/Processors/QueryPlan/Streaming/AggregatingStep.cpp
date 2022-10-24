#include "AggregatingStep.h"

#include <Processors/Transforms/Streaming/GlobalAggregatingTransform.h>
#include <Processors/Transforms/Streaming/SessionAggregatingTransform.h>
#include <Processors/Transforms/Streaming/TumbleHopAggregatingTransform.h>
#include <Processors/Transforms/Streaming/TumbleHopAggregatingTransformWithSubstream.h>

#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{
namespace Streaming
{
namespace
{
ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = false, /// Actually, we may check that distinct names are in aggregation keys
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}
}

AggregatingStep::AggregatingStep(
    const DataStream & input_stream_,
    Aggregator::Params params_,
    bool final_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_,
    bool storage_has_evenly_distributed_read_,
    bool emit_version_)
    : ITransformingStep(
        input_stream_,
        params_.getHeader(final_, params_.group_by == Aggregator::Params::GroupBy::SESSION, params_.time_col_is_datetime64, emit_version_),
        getTraits(),
        false)
    , params(std::move(params_))
    , final(std::move(final_))
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , storage_has_evenly_distributed_read(storage_has_evenly_distributed_read_)
    , emit_version(emit_version_)
{
}

void AggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || params.max_bytes_before_external_group_by != 0;
    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    auto transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), final, emit_version);

    /// For substream
    if (!transform_params->params.substream_key_indices.empty()
        || transform_params->params.group_by == Aggregator::Params::GroupBy::SESSION)
    {
        transformPipelineWithSubstream(transform_params, pipeline, settings);
        aggregating = collector.detachProcessors(0);
        return;
    }

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        if (!storage_has_evenly_distributed_read)
            pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            if (transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
                || transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END)
                return std::make_shared<TumbleHopAggregatingTransform>(
                    header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
            else
                return std::make_shared<GlobalAggregatingTransform>(
                    header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
        });

        pipeline.resize(1);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            if (transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
                || transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END)
                return std::make_shared<TumbleHopAggregatingTransform>(header, transform_params);
            else
                return std::make_shared<GlobalAggregatingTransform>(header, transform_params);
        });
    }

    aggregating = collector.detachProcessors(0);
}

void AggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void AggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void AggregatingStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(aggregating, settings);
}

void AggregatingStep::transformPipelineWithSubstream(
    AggregatingTransformParamsPtr transform_params, QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    assert(
        transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
        || transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END
        || transform_params->params.group_by == Aggregator::Params::GroupBy::SESSION);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        if (!storage_has_evenly_distributed_read)
            pipeline.resize(pipeline.getNumStreams(), true, true);

        auto substream_many_data = std::make_shared<SubstreamManyAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            if (transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
                || transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END)
                return std::make_shared<TumbleHopAggregatingTransformWithSubstream>(
                    header, transform_params, substream_many_data, counter++, merge_threads, temporary_data_merge_threads);
            else
                return std::make_shared<SessionAggregatingTransform>(
                    header, transform_params, substream_many_data, counter++, merge_threads, temporary_data_merge_threads);
        });

        pipeline.resize(1);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            if (transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
                || transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END)
                return std::make_shared<TumbleHopAggregatingTransformWithSubstream>(header, transform_params);
            else
                return std::make_shared<SessionAggregatingTransform>(header, transform_params);
        });
    }
}
}
}
