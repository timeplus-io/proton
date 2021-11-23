#include "StreamingWindowAssignmentTransform.h"

#include <Columns/ColumnArray.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace
{
    ALWAYS_INLINE void setWindowColumnTumble(Block & block, const ColumnTuple * col_tuple, size_t src_pos, Int32 target_pos)
    {
        if (target_pos >= 0)
        {
            assert(block.getByPosition(target_pos).column->getDataType() == col_tuple->getColumnPtr(src_pos)->getDataType());
            block.getByPosition(target_pos).column = std::move(col_tuple->getColumnPtr(src_pos));
        }
    }

    ALWAYS_INLINE void setWindowColumnHop(Block & block, const ColumnTuple * col_tuple, size_t src_pos, Int32 target_pos, bool & replicated)
    {
        if (target_pos >= 0)
        {
            /// const ColumnArray & src = assert_cast<const ColumnArray &>(*wstart_result);
            auto src_win_col = checkAndGetColumn<ColumnArray>(col_tuple->getColumnPtr(src_pos).get());

            if (!replicated)
            {
                const auto & offsets = src_win_col->getOffsets();
                for (auto & column_with_type : block)
                    if (!column_with_type.column->empty())
                        column_with_type.column = column_with_type.column->replicate(offsets);
                replicated = true;
            }

            assert(block.getByPosition(target_pos).column->getDataType() == src_win_col->getDataPtr()->getDataType());
            block.getByPosition(target_pos).column = src_win_col->getDataPtr();
        }
    }
}

StreamingWindowAssignmentTransform::StreamingWindowAssignmentTransform(
    const Block & input_header, const Block & output_header, StreamingFunctionDescriptionPtr desc)
    : ISimpleTransform(input_header, output_header, false), func_desc(std::move(desc)), chunk_header(output_header.getColumns(), 0)
{
    assert(func_desc);

    calculateColumns(input_header, output_header);

    func_name = func_desc->func_ast->as<ASTFunction>()->name;
}

void StreamingWindowAssignmentTransform::transform(Chunk & chunk)
{
    if (chunk.hasRows())
        assignWindow(chunk);
    else
        /// The downstream header is different than the output of this transform
        /// We need use the current output header
        chunk.setColumns(chunk_header.cloneEmptyColumns(), 0);
}

void StreamingWindowAssignmentTransform::assignWindow(Chunk & chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    Block expr_block;

    /// Most of the time, we copied only one column
    for (auto pos : expr_column_positions)
        expr_block.insert(block.getByPosition(pos));

    func_desc->expr->execute(expr_block);

    /// auto * col_with_type = expr_block.findByName(STREAMING_WINDOW_FUNC_ALIAS);
    /// So far we assume, the streaming function produces only one column
    assert(expr_block);

    /// Only select columns required by output
    /// For example, inputs: col1, col2, col3. col3 is used to calculate windows
    /// outputs: col1, col2, window_begin, window_end.
    auto result = getOutputPort().getHeader().cloneEmpty();

    auto wmin_pos = std::min(wstart_pos, wend_pos);
    auto wmax_pos = std::max(wstart_pos, wend_pos);

    /// Insert columns before window_begin or window_end
    for (Int32 i = 0; i < wmin_pos; ++i)
        result.getByPosition(i) = std::move(block.getByPosition(input_column_positions[i]));

    /// Insert columns between window_begin and window_end
    for (Int32 i = wmin_pos + 1; i < wmax_pos; ++i)
        result.getByPosition(i).column = std::move(block.getByPosition(input_column_positions[i]).column);

    /// Insert columns after window_begin or window_end
    size_t delta = wmin_pos >= 0 ? 2 : 1;
    for (size_t i = wmax_pos + 1, num_columns = chunk_header.getNumColumns(); i < num_columns; ++i)
        result.getByPosition(i).column = std::move(block.getByPosition(input_column_positions[i - delta]).column);

    /// Insert window_begin and window_end
    if (func_name == "__TUMBLE")
    {
        assignTumbleWindow(result, expr_block);
    }
    else if (func_name == "__HOP")
    {
        assignHopWindow(result, expr_block);
    }
    else
    {
        throw Exception(func_name + " is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    chunk.setColumns(result.getColumns(), result.rows());
}

ALWAYS_INLINE void StreamingWindowAssignmentTransform::assignTumbleWindow(Block & result, Block & expr_block)
{
    /// Result column
    auto & col_with_type = expr_block.getByPosition(0);

    /// Flatten the tuple
    assert(isTuple(col_with_type.type));
    auto col_tuple = checkAndGetColumn<ColumnTuple>(col_with_type.column.get());

    if (wstart_pos < wend_pos)
    {
        setWindowColumnTumble(result, col_tuple, 0, wstart_pos);
        setWindowColumnTumble(result, col_tuple, 1, wend_pos);
    }
    else
    {
        setWindowColumnTumble(result, col_tuple, 1, wend_pos);
        setWindowColumnTumble(result, col_tuple, 0, wstart_pos);
    }
}

void StreamingWindowAssignmentTransform::assignHopWindow(Block & result, Block & expr_block)
{
    auto & col_with_type = expr_block.getByPosition(0);

    assert(isTuple(col_with_type.type));
    auto col_tuple = checkAndGetColumn<ColumnTuple>(col_with_type.column.get());

    bool replicated = false;
    if (wstart_pos < wend_pos)
    {
        setWindowColumnHop(result, col_tuple, 0, wstart_pos, replicated);
        setWindowColumnHop(result, col_tuple, 1, wend_pos, replicated);
    }
    else
    {
        setWindowColumnHop(result, col_tuple, 1, wend_pos, replicated);
        setWindowColumnHop(result, col_tuple, 0, wstart_pos, replicated);
    }
}

void StreamingWindowAssignmentTransform::calculateColumns(const Block & input_header, const Block & output_header)
{
    expr_column_positions.reserve(func_desc->input_columns.size());

    size_t pos = 0;
    for (const auto & col_with_type : output_header)
    {
        if (col_with_type.name == STREAMING_WINDOW_START)
        {
            wstart_pos = pos;
        }
        else if (col_with_type.name == STREAMING_WINDOW_END)
        {
            wend_pos = pos;
        }
        else
        {
            input_column_positions.push_back(input_header.getPositionByName(col_with_type.name));
        }
        ++pos;
    }

    assert(wstart_pos >= 0 || wend_pos >= 0);

    auto input_begin = func_desc->input_columns.begin();
    auto input_end = func_desc->input_columns.end();

    pos = 0;
    for (const auto & col_with_type : input_header)
    {
        if (std::find(input_begin, input_end, col_with_type.name) != input_end)
            expr_column_positions.push_back(pos);
        ++pos;
    }
}
}
