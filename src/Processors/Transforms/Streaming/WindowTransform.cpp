#include "WindowTransform.h"

#include "Interpreters/Streaming/Aggregator.h"

namespace DB
{
namespace Streaming
{
WindowTransform::WindowTransform(
    const Block & input_header_,
    const Block & output_header_,
    const WindowDescription & window_description_,
    const std::vector<WindowFunctionDescription> & functions)
    : ISimpleTransform(input_header_, output_header_, false, ProcessorID::StreamingWindowTransformID)
{
    // Initialize window function workspaces.
    std::vector<WindowFunctionWorkspace> workspaces;
    workspaces.reserve(functions.size());
    for (const auto & f : functions)
    {
        WindowFunctionWorkspace workspace;
        if (f.function)
        {
            /// Currently we have slightly wrong mixup of the interfaces of Window and Aggregate functions.
            workspace.template_function = f.template_function;
            workspace.template_arguments = f.template_arguments;
            workspace.result_type = f.function->getResultType();

            workspace.argument_column_indices.reserve(f.argument_names.size());
            for (const auto & argument_name : f.argument_names)
                workspace.argument_column_indices.push_back(input_header_.getPositionByName(argument_name));
        }
        else
        {
            assert(false);
            // workspace.result_type = f.aggregate_function->getReturnType();
            // workspace.aggregate_function = f.aggregate_function;
            // const auto & aggregate_function = workspace.aggregate_function;
            // if (!arena && aggregate_function->allocatesMemoryInArena())
            // {
            //     arena = std::make_unique<Arena>();
            // }
            // workspace.aggregate_function_state.reset(aggregate_function->sizeOfData(), aggregate_function->alignOfData());
            // aggregate_function->create(workspace.aggregate_function_state.data());
        }

        workspaces.push_back(std::move(workspace));
    }

    std::vector<size_t> partition_by_indices;
    partition_by_indices.reserve(window_description_.partition_by.size());
    auto partition_by_type = Aggregator::Params::GroupBy::OTHER;
    for (const auto & column : window_description_.partition_by)
    {
        auto key = input_header_.getByName(column.column_name);
        auto pos = input_header_.getPositionByName(column.column_name);
        if ((key.name == ProtonConsts::STREAMING_WINDOW_END) && (isDate(key.type) || isDateTime(key.type) || isDateTime64(key.type)))
        {
            partition_by_indices.insert(partition_by_indices.begin(), pos);
            partition_by_type = Aggregator::Params::GroupBy::WINDOW_END;
        }
        else if (
            (key.name == ProtonConsts::STREAMING_WINDOW_START) && (isDate(key.type) || isDateTime(key.type) || isDateTime64(key.type))
            && (partition_by_type != Aggregator::Params::GroupBy::WINDOW_END))
        {
            partition_by_indices.insert(partition_by_indices.begin(), pos);
            partition_by_type = Aggregator::Params::GroupBy::WINDOW_START;
        }
        else
            partition_by_indices.push_back(pos);
    }

    assert(window_description_.order_by.empty());
    assert(window_description_.frame.is_default);

    assert(input_header_.columns() + workspaces.size() == output_header_.columns());
    /// we want to calc columns and return (all input columns + result columns)
    auto context_indices_v{std::views::iota(0ul, input_header_.columns())};
    std::vector<size_t> context_indices(context_indices_v.begin(), context_indices_v.end());

    /// When the partition keys has window_start or window_end, we apply a two level substream executor, which enables pool recycle
    if (partition_by_type == Aggregator::Params::GroupBy::WINDOW_START || partition_by_type == Aggregator::Params::GroupBy::WINDOW_END)
        executor = std::make_unique<WindowFunctionExecutor>(
            Substream::GroupByKeys::WINDOWED_PARTITION_KEYS,
            input_header_,
            std::move(partition_by_indices),
            std::move(workspaces),
            std::move(context_indices));
    else
        executor = std::make_unique<WindowFunctionExecutor>(
            Substream::GroupByKeys::PARTITION_KEYS,
            input_header_,
            std::move(partition_by_indices),
            std::move(workspaces),
            std::move(context_indices));

    empty_chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
}

void WindowTransform::transform(Chunk & chunk)
{
    if (!chunk.hasRows())
    {
        chunk = empty_chunk.clone();
        return;
    }

    auto rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    auto results = executor->execute(columns, rows);
    chunk.setColumns(std::move(results), rows);
}

}
}
