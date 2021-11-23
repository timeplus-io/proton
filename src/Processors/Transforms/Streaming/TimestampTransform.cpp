#include "TimestampTransform.h"

#include <Common/ProtonCommon.h>

namespace DB
{
TimestampTransform::TimestampTransform(
    const Block & input_header, const Block & output_header, ExpressionActionsPtr timestamp_expr_, const Names & input_columns_)
    : ISimpleTransform(input_header, output_header, false)
    , timestamp_expr(std::move(timestamp_expr_))
    , chunk_header(output_header.getColumns(), 0)
{
    assert(timestamp_expr);

    calculateColumns(input_header, output_header, input_columns_);
}

void TimestampTransform::transform(Chunk & chunk)
{
    if (chunk.hasRows())
        transformTimestamp(chunk);
    else
        /// The downstream header is different than the output of this transform
        /// We need use the current output header
        chunk.setColumns(chunk_header.cloneEmptyColumns(), 0);
}

void TimestampTransform::transformTimestamp(Chunk & chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    Block expr_block;

    /// Most of the time, we copied only one column
    for (auto pos : expr_column_positions)
        expr_block.insert(block.getByPosition(pos));

    timestamp_expr->execute(expr_block);

    /// auto * col_with_type = expr_block.findByName(STREAMING_WINDOW_FUNC_ALIAS);
    /// So far we assume, the streaming function produces only one column
    assert(expr_block);

    /// Only select columns required by output
    /// For example, inputs: col1, col2, col3, col4. col3 and col4 are used to calculate timestamp
    /// outputs: col1, col2, ____ts. `____ts` is the resulting column of the timestamp expression
    auto result = getOutputPort().getHeader().cloneEmpty();

    /// Insert columns before timestamp column
    for (size_t col_pos = 0; col_pos < timestamp_col_pos; ++col_pos)
        result.getByPosition(col_pos) = std::move(block.getByPosition(input_column_positions[col_pos]));

    /// Insert timestamp column
    result.getByPosition(timestamp_col_pos) = std::move(expr_block.getByPosition(0));

    /// Insert columns after timestamp column
    for (size_t col_pos = timestamp_col_pos + 1, num_cols = chunk_header.getNumColumns(); col_pos < num_cols; ++col_pos)
        result.getByPosition(col_pos) =  std::move(block.getByPosition(input_column_positions[col_pos - 1]));

    chunk.setColumns(result.getColumns(), result.rows());
}

void TimestampTransform::calculateColumns(const Block & input_header, const Block & output_header, const Names & input_columns)
{
    expr_column_positions.reserve(input_columns.size());

    /// Calculate the position of STREAMING_TIME_ALIAS column in resulting chunk
    size_t pos = 0;
    for (const auto & col_with_type : output_header)
    {
        if (col_with_type.name == STREAMING_TIMESTAMP_ALIAS)
        {
            timestamp_col_pos = pos;
            timestamp_col_data_type = col_with_type.type;
        }
        else
        {
            input_column_positions.push_back(input_header.getPositionByName(col_with_type.name));
        }
        ++pos;
    }

    /// Calculate the positions of dependent columns in input chunk
    auto input_begin = input_columns.begin();
    auto input_end = input_columns.end();

    pos = 0;
    for (const auto & col_with_type : input_header)
    {
        if (std::find(input_begin, input_end, col_with_type.name) != input_end)
            expr_column_positions.push_back(pos);
        ++pos;
    }
}
}
