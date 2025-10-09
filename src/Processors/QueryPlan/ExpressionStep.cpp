#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Interpreters/JoinSwitcher.h>
#include <Common/JSONBuilder.h>

/// proton: starts.
#include <Processors/Transforms/Streaming/ExpressionTransformWithSubstream.h>
/// proton: ends.

namespace DB
{

static ITransformingStep::Traits getTraits(const ActionsDAGPtr & actions, const Block & header, const SortDescription & sort_description, bool preserves_substream)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = !actions->hasArrayJoin(),
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = actions->isSortingPreserved(header, sort_description),
            /// proton: starts.
            .preserves_substream = preserves_substream,
            /// proton: ends.
        },
        {
            .preserves_number_of_rows = !actions->hasArrayJoin(),
        }
    };
}

/// proton: starts.
ExpressionStep::ExpressionStep(
    const DataStream & input_stream_,
    const ActionsDAGPtr & actions_dag_,
    HashTableType hash_table_type_,
    const std::string & spill_dir_,
    size_t max_hot_keys_,
    bool preserves_substream)
    : ITransformingStep(
          input_stream_,
          ExpressionTransform::transformHeader(input_stream_.header, *actions_dag_),
          getTraits(actions_dag_, input_stream_.header, input_stream_.sort_description, preserves_substream))
    , actions_dag(actions_dag_)
    , hash_table_type(hash_table_type_)
    , spill_dir(spill_dir_)
    , max_hot_keys(max_hot_keys_)
/// proton: ends.
{
    /// Some columns may be removed by expression.
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

void ExpressionStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());

    /// proton: starts.
    bool with_substream = getInputStreams().front().with_substream;
    size_t transform_id = 0;
    pipeline.addSimpleTransform([&](const Block & header) -> ProcessorPtr
    {
        if (with_substream)
        {
            /// We need to deep clone the expression since the stateful functions in the expression will be switched for current substream
            /// in multiple ExpressionTransformWithSubstream at the same time
            if (transform_id > 0)
                expression = expression->deepClone();

            return std::make_shared<ExpressionTransformWithSubstream>(header, expression, hash_table_type, fmt::format("{}-{}", spill_dir, transform_id++), max_hot_keys);
        }
        else
        {
            return std::make_shared<ExpressionTransform>(header, expression);
        }
    });
    /// proton: ends.

    if (!blocksHaveEqualStructure(pipeline.getHeader(), output_stream->header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                output_stream->header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, convert_actions);
        });
    }
}

void ExpressionStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    bool first = true;

    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    for (const auto & action : expression->getActions())
    {
        settings.out << prefix << (first ? "Actions: "
                                         : "         ");
        first = false;
        settings.out << action.toString() << '\n';
    }

    settings.out << prefix << "Positions:";
    for (const auto & pos : expression->getResultPositions())
        settings.out << ' ' << pos;
    settings.out << '\n';
}

void ExpressionStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    map.add("Expression", expression->toTree());
}

void ExpressionStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), ExpressionTransform::transformHeader(input_streams.front().header, *actions_dag), getDataStreamTraits());
}

}
