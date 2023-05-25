#include <Processors/Transforms/Streaming/WindowAssignmentTransform.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Streaming/FunctionDescription.h>

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
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

        Block expr_block;

        /// Most of the time, we copied only one column
        for (auto pos : expr_column_positions)
            expr_block.insert(block.getByPosition(pos));

        window_params->desc->expr->execute(expr_block);

        /// auto * col_with_type = expr_block.findByName(STREAMING_WINDOW_FUNC_ALIAS);
        /// So far we assume, the streaming function produces only one column
        assert(expr_block);

        auto & col_with_type = expr_block.getByPosition(0);
        assert(isTuple(col_with_type.type));

        assert(col_with_type.column);
        auto col = IColumn::mutate(col_with_type.column->convertToFullColumnIfConst());
        auto & col_tuple = assert_cast<ColumnTuple &>(*col);

        /// 1) Assign window function result columns (such as window_start and window_end)
        /// 2) Remove unused columns
        assignWindow(chunk, block.getColumns(), std::move(col_tuple));
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
    window_params->desc->expr->execute(transformed_header);

    auto * tuple_type = checkAndGetDataType<DataTypeTuple>(transformed_header.getByPosition(0).type.get());
    assert(tuple_type && tuple_type->haveExplicitNames());

    output_column_positions.reserve(output_header.columns());
    for (const auto & col_with_type : output_header)
    {
        if (auto tuple_elem_pos = tuple_type->tryGetPositionByName(col_with_type.name); tuple_elem_pos.has_value())
            /// we use negative pos `-1, ... , -n` to indicate tuple elem columns pos (0, ..., n-1)
            output_column_positions.push_back(-1 - tuple_elem_pos.value());
        else
            output_column_positions.push_back(input_header.getPositionByName(col_with_type.name));
    }
}

}
}
