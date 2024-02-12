#include "AggregatingStep.h"

#include <Processors/Transforms/Streaming/GlobalAggregatingTransform.h>
#include <Processors/Transforms/Streaming/HopAggregatingTransform.h>
#include <Processors/Transforms/Streaming/SessionAggregatingTransform.h>
#include <Processors/Transforms/Streaming/TumbleAggregatingTransform.h>
#include <Processors/Transforms/Streaming/UserDefinedEmitStrategyAggregatingTransform.h>

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
    bool emit_version_,
    bool emit_changelog_,
    Streaming::EmitMode watermark_emit_mode_)
    : ITransformingStep(input_stream_, AggregatingTransformParams::getHeader(params_, final_, emit_version_, emit_changelog_), getTraits(), false)
    , params(std::move(params_))
    , final(std::move(final_))
    , merge_threads(merge_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , emit_version(emit_version_)
    , emit_changelog(emit_changelog_)
    , emit_mode(watermark_emit_mode_)
{
}

void AggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (emit_changelog && params.group_by != Aggregator::Params::GroupBy::OTHER)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only support streaming global aggregation emit changelog");

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
    auto transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), final, emit_version, emit_changelog, emit_mode);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.getNumStreams() > 1)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        /// For streaming aggregating, we don't need use StrictResizeProcessor.
        /// There is no case that some upstream closed, since AggregatingTransform required `watermark` of upstream to trigger finalize.
        // if (!storage_has_evenly_distributed_read)
        //     pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyAggregatedData>(pipeline.getNumStreams());

        size_t counter = 0;
        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            if (transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
                || transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END)
            {
                assert(transform_params->params.window_params);
                switch (transform_params->params.window_params->type)
                {
                    case WindowType::TUMBLE:
                        return std::make_shared<TumbleAggregatingTransform>(
                            header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
                    case WindowType::HOP:
                        return std::make_shared<HopAggregatingTransform>(
                            header, transform_params, many_data, counter++, merge_threads, temporary_data_merge_threads);
                    case WindowType::SESSION:
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Parallel processing session window is not supported");
                    default:
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "No support window type: {}",
                            magic_enum::enum_name(transform_params->params.window_params->type));
                }
            }
            else if (transform_params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
                return std::make_shared<UserDefinedEmitStrategyAggregatingTransform>(
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
            {
                assert(transform_params->params.window_params);
                switch (transform_params->params.window_params->type)
                {
                    case WindowType::TUMBLE:
                        return std::make_shared<TumbleAggregatingTransform>(header, transform_params);
                    case WindowType::HOP:
                        return std::make_shared<HopAggregatingTransform>(header, transform_params);
                    case WindowType::SESSION:
                        return std::make_shared<SessionAggregatingTransform>(header, transform_params);
                    default:
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "No support window type: {}",
                            magic_enum::enum_name(transform_params->params.window_params->type));
                }
            }
            else if (transform_params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
                return std::make_shared<UserDefinedEmitStrategyAggregatingTransform>(header, transform_params);
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

}
}
