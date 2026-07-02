#include <Processors/QueryPlan/Streaming/AggregatingStep.h>

#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>
#include <Processors/Transforms/Streaming/EmitParams.h>
#include <Processors/Transforms/Streaming/GlobalAggregatingTransform.h>
#include <Processors/Transforms/Streaming/GlobalAggregatingTransformWithSessionKey.h>
#include <Processors/Transforms/Streaming/HopAggregatingTransform.h>
#include <Processors/Transforms/Streaming/SessionAggregatingTransform.h>
#include <Processors/Transforms/Streaming/TumbleAggregatingTransform.h>
#include <Processors/Transforms/Streaming/UserDefinedEmitStrategyAggregatingTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
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
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
            .preserves_shuffling = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

}

AggregatingStep::AggregatingStep(
    const DataStream & input_stream_,
    IAggregatorParamsPtr params_,
    std::shared_ptr<const Streaming::EmitParams> emit_params_,
    size_t merge_threads_,
    bool emit_version_,
    bool emit_changelog_,
    bool keys_already_sharded_,
    bool aggregation_backfill_key_unique_)
    : ITransformingStep(
          input_stream_,
          AggregatingTransformParams::getHeader(input_stream_.header, *params_, emit_version_, emit_changelog_),
          getTraits(),
          false)
    , params(std::move(params_))
    , emit_params(std::move(emit_params_))
    , merge_threads(merge_threads_)
    , emit_version(emit_version_)
    , emit_changelog(emit_changelog_)
    , keys_already_sharded(keys_already_sharded_)
    , aggregation_backfill_key_unique(aggregation_backfill_key_unique_)
{
}

void AggregatingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (emit_changelog)
    {
        if (params->group_by != IAggregatorParams::GroupBy::Other)
            throw Exception(ErrorCodes::UNSUPPORTED, "EMIT CHANGELOG is only supported in Streaming Global Aggregation");

        if (emit_params->mode == EmitMode::AfterSessionClose)
            throw Exception(ErrorCodes::UNSUPPORTED, "EMIT CHANGELOG with AFTER KEY EXPIRE is not supported");

        if (emit_params->mode == EmitMode::PerEvent)
            throw Exception(ErrorCodes::UNSUPPORTED, "EMIT CHANGELOG with PER EVENT is not supported");
    }

    if (emit_params->mode == EmitMode::AfterSessionClose && params->group_by != IAggregatorParams::GroupBy::Other)
        throw Exception(ErrorCodes::UNSUPPORTED, "'EMIT AFTER SESSION CLOSE' can only be used in global aggregation");

    if (pipeline.getNumStreams() > 1 && !keys_already_sharded)
    {
        if (emit_params->mode == EmitMode::AfterSessionClose)
            throw Exception(
                ErrorCodes::UNSUPPORTED,
                "Data shuffling is required for multiple shards for `EMIT AFTER SESSION CLOSE`. Shuffle data by `shuffle by` or use query "
                "setting allow_independent_shard_processing=true if the data is already shuffled. For example, SELECT ... SHUFFLE BY "
                "key_col1, key_col2, ... GROUP BY key_co1, key_col2, ... EMIT AFTER SESSION CLOSE...;");

        if (emit_params->mode == EmitMode::PerEvent)
            throw Exception(
                ErrorCodes::UNSUPPORTED,
                "Data shuffling is required for multiple shards for `EMIT PER EVENT`. Shuffle data by `shuffle by` or use query setting "
                "allow_independent_shard_processing=true if the data is already shuffled. For example, SELECT ... SHUFFLE BY key_col1, "
                "key_col2, ... GROUP BY key_col1, key_col2, ... EMIT PER EVENT ...;");

        if (emit_params->window_params && emit_params->window_params->type == WindowType::Session)
            throw Exception(
                ErrorCodes::UNSUPPORTED,
                "Data shuffling is required for multiple shards for session window. Shuffle data by `shuffle by` or use query setting "
                "allow_independent_shard_processing=true if the data is already shuffled. For example, SELECT ... SHUFFLE BY key_col1, "
                "key_col2, ... GROUP BY key_col1, key_col2, ...;");

        if (params->group_by == IAggregatorParams::GroupBy::UserDefined)
            throw Exception(
                ErrorCodes::UNSUPPORTED,
                "Data shuffling is required for multiple shards for user-defined emit strategy. Shuffle data by `shuffle by` or use query "
                "setting allow_independent_shard_processing=true if the data is already shuffled. For example, SELECT ... SHUFFLE BY "
                "key_col1, key_col2, ... GROUP BY key_col1, key_col2, ...;");
    }

    QueryPipelineProcessorsCollector collector(pipeline, this);

    /// Forget about current totals and extremes. They will be calculated again after aggregation if needed.
    pipeline.dropTotalsAndExtremes();

    if (params->aggregatorType() == AggregatorType::Memory)
    {
        auto mem_params = static_cast<MemoryAggregatorParams *>(params.get());
        bool allow_to_use_two_level_group_by = pipeline.getNumStreams() > 1 || mem_params->max_bytes_before_external_group_by != 0;
        if (!allow_to_use_two_level_group_by)
        {
            mem_params->group_by_two_level_threshold = 0;
            mem_params->group_by_two_level_threshold_bytes = 0;
        }
    }

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    auto transform_params = std::make_shared<AggregatingTransformParams>(
        input_streams.front().header,
        params,
        emit_version,
        emit_changelog,
        emit_params->repeat,
        emit_params->delta,
        emit_params->mode,
        keys_already_sharded,
        aggregation_backfill_key_unique);

    /// If there are multiple inputs, parallel aggregation is performed using shared ManyAggregatedData.
    /// Exception: If the data is already shuffled, aggregation can be performed independently for each input (share nothing).
    if (pipeline.getNumStreams() > 1 && !keys_already_sharded)
    {
        /// Add resize transform to uniformly distribute data between aggregating streams.
        /// For streaming aggregating, we don't need use StrictResizeProcessor.
        /// There is no case that some upstream closed, since AggregatingTransform required `watermark` of upstream to trigger finalize.
        // if (!storage_has_evenly_distributed_read)
        //     pipeline.resize(pipeline.getNumStreams(), true, true);

        auto many_data = std::make_shared<ManyAggregatedData>(params->aggregatorType(), pipeline.getNumStreams());

        size_t transform_id = 0;
        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            if (transform_params->params->group_by == IAggregatorParams::GroupBy::WindowStart
                || transform_params->params->group_by == IAggregatorParams::GroupBy::WindowEnd)
            {
                assert(transform_params->params->window_params);
                switch (transform_params->params->window_params->type)
                {
                    case WindowType::Tumble:
                        return std::make_shared<TumbleAggregatingTransform>(
                            header, transform_params, many_data, transform_id++, merge_threads);
                    case WindowType::Hop:
                        return std::make_shared<HopAggregatingTransform>(
                            header, transform_params, many_data, transform_id++, merge_threads);
                    case WindowType::Session:
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Parallel processing session window is not supported");
                    default:
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "No support window type: {}",
                            magic_enum::enum_name(transform_params->params->window_params->type));
                }
            }
            else if (transform_params->params->group_by == IAggregatorParams::GroupBy::UserDefined)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "User-defined emit strategy aggregation is not supported");
            else
                return std::make_shared<GlobalAggregatingTransform>(header, transform_params, many_data, transform_id++, merge_threads);
        });

        pipeline.resize(1);
    }
    else
    {
        size_t transform_id = 0;
        pipeline.addSimpleTransform([&](const Block & header) -> std::shared_ptr<IProcessor> {
            if (transform_params->params->group_by == IAggregatorParams::GroupBy::WindowStart
                || transform_params->params->group_by == IAggregatorParams::GroupBy::WindowEnd)
            {
                chassert(transform_params->params->window_params);
                switch (transform_params->params->window_params->type)
                {
                    case WindowType::Tumble:
                        return std::make_shared<TumbleAggregatingTransform>(header, transform_params, std::to_string(transform_id++));
                    case WindowType::Hop:
                        return std::make_shared<HopAggregatingTransform>(header, transform_params, std::to_string(transform_id++));
                    case WindowType::Session:
                        return std::make_shared<SessionAggregatingTransform>(header, transform_params, std::to_string(transform_id++));
                    default:
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Unknown window type: {}",
                            magic_enum::enum_name(transform_params->params->window_params->type));
                }
            }
            else if (transform_params->params->group_by == IAggregatorParams::GroupBy::UserDefined)
                return std::make_shared<UserDefinedEmitStrategyAggregatingTransform>(
                    header, transform_params, std::to_string(transform_id++));
            else if (emit_params->mode == EmitMode::AfterSessionClose)
                return std::make_shared<GlobalAggregatingTransformWithSessionKey>(header, transform_params, std::to_string(transform_id++));
            else
                return std::make_shared<GlobalAggregatingTransform>(header, transform_params, std::to_string(transform_id++));
        });
    }

    aggregating = collector.detachProcessors(0);
}

void AggregatingStep::describeActions(FormatSettings & settings) const
{
    params->explain(settings.out, settings.offset);
}

void AggregatingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    params->explain(map);
}

void AggregatingStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(aggregating, settings);
}

void AggregatingStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        AggregatingTransformParams::getHeader(input_streams.front().header, *params, emit_version, emit_changelog),
        getDataStreamTraits());
}

String AggregatingStep::getName() const
{
    if (params->group_by == IAggregatorParams::GroupBy::WindowStart || params->group_by == IAggregatorParams::GroupBy::WindowEnd)
    {
        chassert(params->window_params);
        switch (params->window_params->type)
        {
            case WindowType::Tumble:
            {
                switch (params->aggregatorType())
                {
                    case AggregatorType::Memory:
                        return "TumbleStreamingAggregating";
                    case AggregatorType::Hybrid:
                        return "HybridTumbleStreamingAggregating";
                }
            }
            case WindowType::Hop:
            {
                switch (params->aggregatorType())
                {
                    case AggregatorType::Memory:
                        return "HopStreamingAggregating";
                    case AggregatorType::Hybrid:
                        return "HybridHopStreamingAggregating";
                }
            }
            case WindowType::Session:
            {
                switch (params->aggregatorType())
                {
                    case AggregatorType::Memory:
                        return "SessionStreamingAggregating";
                    case AggregatorType::Hybrid:
                        return "HybridSessionStreamingAggregating";
                }
            }
            default:
            {
                switch (params->aggregatorType())
                {
                    case AggregatorType::Memory:
                        return "StreamingAggregating";
                    case AggregatorType::Hybrid:
                        return "HybridStreamingAggregating";
                }
            }
        }
    }
    else if (params->group_by == IAggregatorParams::GroupBy::UserDefined)
    {
        switch (params->aggregatorType())
        {
            case AggregatorType::Memory:
                return "UserDefinedStreamingAggregating";
            case AggregatorType::Hybrid:
                return "HybridUserDefinedStreamingAggregating";
        }
    }
    else
    {
        switch (params->aggregatorType())
        {
            case AggregatorType::Memory:
                return "GlobalStreamingAggregating";
            case AggregatorType::Hybrid:
            {
                if (emit_params->mode == EmitMode::AfterSessionClose)
                    return "HybridGlobalStreamingAggregatingWithSessionKey";
                else
                    return "HybridGlobalStreamingAggregating";
            }
        }
    }
}

}

}
