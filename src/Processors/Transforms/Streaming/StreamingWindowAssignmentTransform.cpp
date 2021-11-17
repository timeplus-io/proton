#include "StreamingWindowAssignmentTransform.h"

#include <Columns/ColumnArray.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Common/StreamingCommon.h>

namespace DB
{
namespace
{
    ALWAYS_INLINE void insertWindowColumnTumble(
        Block & block,
        const ColumnTuple * col_tuple,
        size_t src_pos,
        const DataTypePtr & col_data_type,
        Int32 target_pos,
        const String & col_name)
    {
        if (target_pos >= 0)
        {
            /// Make a `window_xxx` column and insert it
            auto src_win_col = col_tuple->getColumnPtr(src_pos);
            assert(col_data_type->getTypeId() == src_win_col->getDataType());
            block.insert(target_pos, ColumnWithTypeAndName{src_win_col, col_data_type, col_name});
        }
    }

    ALWAYS_INLINE void insertWindowColumnHop(
        Block & block,
        const ColumnTuple * col_tuple,
        size_t src_pos,
        const DataTypePtr & col_data_type,
        Int32 target_pos,
        const String & col_name,
        bool & replicated)
    {
        if (target_pos >= 0)
        {
            /// const ColumnArray & src = assert_cast<const ColumnArray &>(*wstart_result);
            auto src_win_col = checkAndGetColumn<ColumnArray>(col_tuple->getColumnPtr(src_pos).get());

            if (!replicated)
            {
                const auto & offsets = src_win_col->getOffsets();
                for (auto & column_with_type : block)
                    column_with_type.column = column_with_type.column->replicate(offsets);
                replicated = true;
            }

            /// Make a `window_xxx` column and insert it
            const auto & src_win_col_data = src_win_col->getDataPtr();
            assert(col_data_type->getTypeId() == src_win_col_data->getDataType());
            block.insert(target_pos, ColumnWithTypeAndName{src_win_col_data, col_data_type, col_name});
        }
    }
}

StreamingWindowAssignmentTransform::StreamingWindowAssignmentTransform(
    const Block & input_header, const Block & output_header, StreamingFunctionDescriptionPtr desc)
    : ISimpleTransform(input_header, output_header, false), func_desc(std::move(desc))
    , chunk_header(output_header.getColumns(), 0)
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

    if (func_name == "__TUMBLE")
    {
        assignTumbleWindow(block, expr_block);
    }
    else if (func_name == "__HOP")
    {
        assignHopWindow(block, expr_block);
    }
    else
    {
        throw Exception(func_name + " is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    chunk.setColumns(block.getColumns(), block.rows());
}

ALWAYS_INLINE void StreamingWindowAssignmentTransform::assignTumbleWindow(Block & block, Block & expr_block)
{
    /// Result column
    auto & col_with_type = expr_block.getByPosition(0);

    /// Flatten the tuple
    assert(isTuple(col_with_type.type));
    auto col_tuple = checkAndGetColumn<ColumnTuple>(col_with_type.column.get());

    if (wstart_pos < wend_pos)
    {
        insertWindowColumnTumble(block, col_tuple, 0, window_start_col_data_type, wstart_pos, STREAMING_WINDOW_START);
        insertWindowColumnTumble(block, col_tuple, 1, window_end_col_data_type, wend_pos, STREAMING_WINDOW_END);
    }
    else
    {
        insertWindowColumnTumble(block, col_tuple, 1, window_end_col_data_type, wend_pos, STREAMING_WINDOW_END);
        insertWindowColumnTumble(block, col_tuple, 0, window_start_col_data_type, wstart_pos, STREAMING_WINDOW_START);
    }
}

void StreamingWindowAssignmentTransform::assignHopWindow(Block & block, Block & expr_block)
{
    auto & col_with_type = expr_block.getByPosition(0);

    assert(isTuple(col_with_type.type));
    auto col_tuple = checkAndGetColumn<ColumnTuple>(col_with_type.column.get());

    bool replicated = false;
    if (wstart_pos < wend_pos)
    {
        insertWindowColumnHop(block, col_tuple, 0, window_start_col_data_type, wstart_pos, STREAMING_WINDOW_START, replicated);
        insertWindowColumnHop(block, col_tuple, 1, window_end_col_data_type, wend_pos, STREAMING_WINDOW_END, replicated);
    }
    else
    {
        insertWindowColumnHop(block, col_tuple, 1, window_end_col_data_type, wend_pos, STREAMING_WINDOW_END, replicated);
        insertWindowColumnHop(block, col_tuple, 0, window_start_col_data_type, wstart_pos, STREAMING_WINDOW_START, replicated);
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
            window_start_col_data_type = col_with_type.type;
        }
        else if (col_with_type.name == STREAMING_WINDOW_END)
        {
            wend_pos = pos;
            window_end_col_data_type = col_with_type.type;
        }
        ++pos;
    }

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
