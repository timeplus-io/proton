#include "AggregatingStepWithSubstream.h"

#include <Processors/Transforms/Streaming/GlobalAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/HopAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/SessionAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/TumbleAggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/UserDefinedEmitStrategyAggregatingTransformWithSubstream.h>

#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{
namespace
{
ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .preserves_distinct_columns = false, /// Actually, we may check that distinct names are in aggregation keys
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}
}

AggregatingStepWithSubstream::AggregatingStepWithSubstream(
    const DataStream & input_stream_, Aggregator::Params params_, bool final_, bool emit_version_, bool emit_changelog_, WatermarkEmitMode watermark_emit_mode_)
    : ITransformingStep(
        input_stream_, AggregatingTransformParams::getHeader(params_, final_, emit_version_, emit_changelog_), getTraits(), false)
    , params(std::move(params_))
    , final(std::move(final_))
    , emit_version(emit_version_)
    , emit_changelog(emit_changelog_)
    , watermark_emit_mode(watermark_emit_mode_)
{
}

void AggregatingStepWithSubstream::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (emit_changelog && params.group_by != Aggregator::Params::GroupBy::OTHER)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only support streaming global aggregation emit changelog");

    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    bool allow_to_use_two_level_group_by = params.max_bytes_before_external_group_by != 0;
    if (!allow_to_use_two_level_group_by)
    {
        params.group_by_two_level_threshold = 0;
        params.group_by_two_level_threshold_bytes = 0;
    }

    auto transform_params = std::make_shared<AggregatingTransformParams>(std::move(params), final, emit_version, emit_changelog, watermark_emit_mode);

    /// If there are several sources, we perform aggregation separately (Assume it's shuffled data by substream keys)
    pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
        if (transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_START
            || transform_params->params.group_by == Aggregator::Params::GroupBy::WINDOW_END)
        {
            assert(transform_params->params.window_params);
            switch (transform_params->params.window_params->type)
            {
                case WindowType::TUMBLE:
                    return std::make_shared<TumbleAggregatingTransformWithSubstream>(header, transform_params);
                case WindowType::HOP:
                    return std::make_shared<HopAggregatingTransformWithSubstream>(header, transform_params);
                case WindowType::SESSION:
                    return std::make_shared<SessionAggregatingTransformWithSubstream>(header, transform_params);
                default:
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "No support window type: {}",
                        magic_enum::enum_name(transform_params->params.window_params->type));
            }
        }
        else if (transform_params->params.group_by == Aggregator::Params::GroupBy::USER_DEFINED)
            return std::make_shared<UserDefinedEmitStrategyAggregatingTransformWithSubstream>(header, transform_params);
        else
            return std::make_shared<GlobalAggregatingTransformWithSubstream>(header, transform_params);
    });

    aggregating = collector.detachProcessors(0);
}

void AggregatingStepWithSubstream::describeActions(FormatSettings & settings) const
{
    params.explain(settings.out, settings.offset);
}

void AggregatingStepWithSubstream::describeActions(JSONBuilder::JSONMap & map) const
{
    params.explain(map);
}

void AggregatingStepWithSubstream::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(aggregating, settings);
}

}
}
