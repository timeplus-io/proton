#include <Processors/Transforms/Streaming/WindowAssignmentTransform.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace Streaming
{
WindowAssignmentTransform::WindowAssignmentTransform(
    const Block & input_header, const Block & output_header, WindowParamsPtr window_params_, ProcessorID pid_)
    : ISimpleTransform(input_header, output_header, false, pid_)
    , chunk_header(output_header.getColumns(), 0)
    , window_params(std::move(window_params_))
{
    assert(window_params);

    calculateColumns(input_header, output_header);
}

void WindowAssignmentTransform::transform(Chunk & chunk)
{
    if (chunk.hasRows())
    {
        auto input_block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

        Block transformed_block;

        /// Most of the time, we copied only one column
        for (auto pos : expr_column_positions)
            transformed_block.insert(input_block.getByPosition(pos));

        window_params->desc->expr_before_table_function->execute(transformed_block);

        assert(transformed_block);

        Columns transformed_columns;
        for (auto & col_with_type : transformed_block)
            transformed_columns.emplace_back(std::move(col_with_type.column));

        /// Assign window function result columns (such as window_start and window_end)
        /// @params: transformed_columns:
        /// <args columns> => <args columns> + <window_start> + <window_end>
        assignWindow(transformed_columns);

        Columns res;
        res.reserve(output_column_positions.size());
        for (auto pos : output_column_positions)
        {
            if (pos < 0)
                res.push_back(std::move(transformed_columns[-1 - pos]));
            else
                res.push_back(std::move(input_block.getByPosition(pos).column));
        }
        auto num_rows = res.at(0)->size();
        chunk.setColumns(std::move(res), num_rows);
    }
    else
    {
        /// The downstream header is different than the output of this transform
        /// We need use the current output header
        chunk.setColumns(chunk_header.cloneEmptyColumns(), 0);
    }
}

void WindowAssignmentTransform::calculateColumns(const Block & input_header, const Block & output_header)
{
    expr_column_positions.reserve(window_params->desc->input_columns.size());

    /// Calculate the positions of dependent columns in input chunk
    for (const auto & col_name : window_params->desc->input_columns)
        expr_column_positions.push_back(input_header.getPositionByName(col_name));

    /// Generate assign window function
    auto transformed_header = input_header.cloneEmpty();
    window_params->desc->expr_before_table_function->execute(transformed_header);
    transformed_header.insert({window_params->desc->argument_types[0], ProtonConsts::STREAMING_WINDOW_START});
    transformed_header.insert({window_params->desc->argument_types[0], ProtonConsts::STREAMING_WINDOW_END});

    output_column_positions.reserve(output_header.columns());
    for (const auto & col_with_type : output_header)
    {
        if (transformed_header.has(col_with_type.name))
            /// we use negative pos `-1, ... , -n` to indicate transformed columns pos (0, ..., n-1)
            output_column_positions.push_back(-1 - transformed_header.getPositionByName(col_with_type.name));
        else
            output_column_positions.push_back(input_header.getPositionByName(col_with_type.name));
    }
}

}
}
