#include <Processors/QueryPlan/Streaming/StreamingAggregatingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

/// proton: starts
#include <Processors/Transforms/Streaming/StreamingAggregatingTransform.h>
/// proton: ends

namespace DB
{
static ITransformingStep::Traits getTraits()
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

StreamingAggregatingStep::StreamingAggregatingStep(
    const DataStream & input_stream_,
    StreamingAggregator::Params params_,
    bool final_,
    size_t merge_threads_,
    size_t temporary_data_merge_threads_,
    bool storage_has_evenly_distributed_read_)
    : ITransformingStep(
        input_stream_,
        params_.getHeader(final_, params_.group_by == StreamingAggregator::Params::GroupBy::SESSION, params_.time_col_is_datetime64),
        getTraits(),
        false)
    , params(std::move(params_))
    , final(std::move(final_))
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , storage_has_evenly_distributed_read(storage_has_evenly_distributed_read_)
{
}

void StreamingAggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
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
    auto transform_params = std::make_shared<StreamingAggregatingTransformParams>(std::move(params), final);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        if (!storage_has_evenly_distributed_read)
            pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyStreamingAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            return std::make_shared<StreamingAggregatingTransform>(
                header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
        });

        pipeline.resize(1);

        aggregating = collector.detachProcessors(0);
    }
    else
    {
        pipeline.resize(1);

        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            return std::make_shared<StreamingAggregatingTransform>(header, transform_params);
        });

        aggregating = collector.detachProcessors(0);
    }
}

void StreamingAggregatingStep::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void StreamingAggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void StreamingAggregatingStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(aggregating, settings);
}

}
