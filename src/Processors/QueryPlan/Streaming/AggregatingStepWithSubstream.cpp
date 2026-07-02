#include <Processors/QueryPlan/Streaming/AggregatingStepWithSubstream.h>

#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>
#include <Processors/Transforms/Streaming/EmitParams.h>
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
extern const int UNSUPPORTED;
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
            .preserves_shuffling = true,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

}

AggregatingStepWithSubstream::AggregatingStepWithSubstream(
    const DataStream & input_stream_,
    IAggregatorParamsPtr params_,
    std::shared_ptr<const Streaming::EmitParams> emit_params_,
    bool emit_version_,
    bool emit_changelog_,
    bool aggregation_backfill_key_unique_)
    : ITransformingStep(
          input_stream_,
          AggregatingTransformParams::getHeader(input_stream_.header, *params_, emit_version_, emit_changelog_),
          getTraits(),
          false)
    , params(std::move(params_))
    , emit_params(std::move(emit_params_))
    , emit_version(emit_version_)
    , emit_changelog(emit_changelog_)
    , aggregation_backfill_key_unique(aggregation_backfill_key_unique_)
{
}

void AggregatingStepWithSubstream::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (emit_changelog && params->group_by != IAggregatorParams::GroupBy::Other)
        throw Exception(ErrorCodes::UNSUPPORTED, "Only support streaming global aggregation emit changelog");

    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    if (params->aggregatorType() == AggregatorType::Memory)
    {
        auto mem_params = static_cast<MemoryAggregatorParams *>(params.get());
        bool allow_to_use_two_level_group_by = mem_params->max_bytes_before_external_group_by != 0;
        if (!allow_to_use_two_level_group_by)
        {
            mem_params->group_by_two_level_threshold = 0;
            mem_params->group_by_two_level_threshold_bytes = 0;
        }
    }

    auto transform_params = std::make_shared<AggregatingTransformParams>(
        input_streams.front().header,
        std::move(params),
        emit_version,
        emit_changelog,
        /*emit_repeat_=*/false,
        emit_params->delta,
        emit_params->mode,
        /*keys_already_sharded_=*/true,
        aggregation_backfill_key_unique);

    if (transform_params->aggregatorType() == AggregatorType::Hybrid)
        throw Exception(ErrorCodes::UNSUPPORTED, "Substream aggregating does not support hybrid aggregator, use memory aggregator instead");

    if (transform_params->emitOnExecute())
        throw Exception(ErrorCodes::UNSUPPORTED, "Substream aggregating with emit mode '{}' is not supported", transform_params->emit_mode);

    /// If there are several sources, we perform aggregation separately (Assume it's shuffled data by substream keys)
    size_t transform_id = 0;
    pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
        if (transform_params->params->group_by == IAggregatorParams::GroupBy::WindowStart
            || transform_params->params->group_by == IAggregatorParams::GroupBy::WindowEnd)
        {
            chassert(transform_params->params->window_params);
            switch (transform_params->params->window_params->type)
            {
                case WindowType::Tumble:
                    return std::make_shared<TumbleAggregatingTransformWithSubstream>(header, transform_params, transform_id++);
                case WindowType::Hop:
                    return std::make_shared<HopAggregatingTransformWithSubstream>(header, transform_params, transform_id++);
                case WindowType::Session:
                    return std::make_shared<SessionAggregatingTransformWithSubstream>(header, transform_params, transform_id++);
                default:
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "No support window type: {}",
                        magic_enum::enum_name(transform_params->params->window_params->type));
            }
        }
        else if (transform_params->params->group_by == IAggregatorParams::GroupBy::UserDefined)
            return std::make_shared<UserDefinedEmitStrategyAggregatingTransformWithSubstream>(header, transform_params, transform_id++);
        else
            return std::make_shared<GlobalAggregatingTransformWithSubstream>(header, transform_params, transform_id++);
    });

    aggregating = collector.detachProcessors(0);
}

void AggregatingStepWithSubstream::describeActions(FormatSettings & settings) const
{
    params->explain(settings.out, settings.offset);
}

void AggregatingStepWithSubstream::describeActions(JSONBuilder::JSONMap & map) const
{
    params->explain(map);
}

void AggregatingStepWithSubstream::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(aggregating, settings);
}

void AggregatingStepWithSubstream::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        AggregatingTransformParams::getHeader(input_streams.front().header, *params, emit_version, emit_changelog),
        getDataStreamTraits());
}

}
}
