#include <Processors/QueryPlan/JoinStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool joinPreservesLeftShuffle(JoinKind kind)
{
    return isInner(kind) || isLeft(kind) || isCrossOrComma(kind);
}

namespace
{

std::vector<std::pair<String, String>> describeJoinActions(const JoinPtr & join)
{
    std::vector<std::pair<String, String>> description;
    const auto & table_join = join->getTableJoin();

    description.emplace_back("Type", toString(table_join.kind()));
    description.emplace_back("Strictness", toString(table_join.strictness()));
    description.emplace_back("Algorithm", join->getName());

    if (table_join.strictness() == JoinStrictness::Asof)
        description.emplace_back("ASOF inequality", toString(table_join.getAsofInequality()));

    if (!table_join.getClauses().empty())
        description.emplace_back("Clauses", table_join.formatClauses(table_join.getClauses(), true /*short_format*/));

    return description;
}

void preserveLeftShuffleDescriptionIfValid(DataStream & output_stream, const DataStream & left_stream, JoinKind kind)
{
    if (!left_stream.shuffle_description || !joinPreservesLeftShuffle(kind))
        return;

    /// Require each key to be a genuine left-input column that also survives in the output, so a
    /// cross-boundary description can't latch onto a same-named right/join-added output column.
    const auto & keys = left_stream.shuffle_description->keys;
    if (!std::ranges::all_of(keys, [&](const auto & key) { return left_stream.header.has(key) && output_stream.header.has(key); }))
        return;

    output_stream.shuffle_description = left_stream.shuffle_description;
}

}

JoinStep::JoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    bool keep_left_read_in_order_)
    : join(std::move(join_)), max_block_size(max_block_size_), max_streams(max_streams_), keep_left_read_in_order(keep_left_read_in_order_)
{
    input_streams = {left_stream_, right_stream_};
    output_stream = DataStream
    {
        .header = JoiningTransform::transformHeader(left_stream_.header, join),
        /// proton: starts. Propagate streaming flag to output stream
        .is_streaming = left_stream_.is_streaming || right_stream_.is_streaming,
        /// proton: ends.
    };
    preserveLeftShuffleDescriptionIfValid(*output_stream, left_stream_, join->getTableJoin().kind());
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    if (join->pipelineType() == JoinPipelineType::YShaped)
    {
        auto joined_pipeline = QueryPipelineBuilder::joinPipelinesYShaped(
            std::move(pipelines[0]), std::move(pipelines[1]), join, output_stream->header, max_block_size, &processors);
        joined_pipeline->resize(max_streams);
        return joined_pipeline;
    }

    return QueryPipelineBuilder::joinPipelinesRightLeft(
            std::move(pipelines[0]),
            std::move(pipelines[1]),
            join,
            output_stream->header,
            max_block_size,
            max_streams,
            keep_left_read_in_order,
            &processors);
}

bool JoinStep::allowPushDownToRight() const
{
    return join->pipelineType() == JoinPipelineType::YShaped || join->pipelineType() == JoinPipelineType::FillRightFirst;
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void JoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    for (const auto & [name, value] : describeJoinActions(join))
        settings.out << prefix << name << ": " << value << '\n';
}

void JoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join))
        map.add(name, value);
}

void JoinStep::updateInputStream(const DataStream & new_input_stream_, size_t idx)
{
    if (idx == 0)
    {
        input_streams = {new_input_stream_, input_streams.at(1)};
        output_stream = DataStream
            {
                .header = JoiningTransform::transformHeader(new_input_stream_.header, join),
                /// proton: starts. Propagate streaming flag to output stream
                .is_streaming = new_input_stream_.is_streaming,
                /// proton: ends.
            };
        preserveLeftShuffleDescriptionIfValid(*output_stream, new_input_stream_, join->getTableJoin().kind());
    }
    else
    {
        /// proton: starts. Right side of the join is always a non-streaming input
        assert(!new_input_stream_.is_streaming);
        /// proton: ends.
        input_streams = {input_streams.at(0), new_input_stream_};
    }
}

static ITransformingStep::Traits getStorageJoinTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
            /// proton: starts.
            .preserves_shuffling = false,
            /// proton: ends.
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilledJoinStep::FilledJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(
        input_stream_,
        JoiningTransform::transformHeader(input_stream_.header, join_),
        getStorageJoinTraits())
    , join(std::move(join_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FilledJoinStep expects Join to be filled");
}

void FilledJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    bool default_totals = false;
    if (!pipeline.hasTotals() && join->getTotals())
    {
        pipeline.addDefaultTotals();
        default_totals = true;
    }

    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(pipeline.getNumStreams());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        auto counter = on_totals ? nullptr : finish_counter;
        return std::make_shared<JoiningTransform>(header, output_stream->header, join, max_block_size, on_totals, default_totals, counter);
    });
}

void FilledJoinStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), JoiningTransform::transformHeader(input_streams.front().header, join), getDataStreamTraits());
}

void FilledJoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    for (const auto & [name, value] : describeJoinActions(join))
        settings.out << prefix << name << ": " << value << '\n';
}

void FilledJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join))
        map.add(name, value);
}

}
