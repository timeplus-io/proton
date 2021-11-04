#include "StreamingWindowAssignmentBlockInputStream.h"

#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Columns/ColumnArray.h>

namespace DB
{
StreamingWindowAssignmentBlockInputStream::StreamingWindowAssignmentBlockInputStream(
    BlockInputStreamPtr input_,
    const Names & column_names,
    StreamingFunctionDescriptionPtr desc,
    ContextPtr context_)
    : input(input_)
    , context(context_)
    , func_desc(desc)
{
    assert(input);
    assert(func_desc);

    calculateColumns(column_names);

    func_name = func_desc->func_ast->as<ASTFunction>()->name;
}

Block StreamingWindowAssignmentBlockInputStream::readImpl()
{
    if (isCancelled())
        return {};

    Block block = input->read();
    if (block.rows())
        assignWindow(block);

    return block;
}

void StreamingWindowAssignmentBlockInputStream::assignWindow(Block & block)
{
    Block expr_block;

    /// Most of the time, we copied only one column
    for (auto pos : expr_column_positions)
        expr_block.insert(block.getByPosition(pos));

    func_desc->expr->execute(expr_block);

    /// auto * col_with_type = expr_block.findByName("____SWIN");
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
}

void StreamingWindowAssignmentBlockInputStream::assignTumbleWindow(Block & block, Block & expr_block)
{
    auto & col_with_type = expr_block.getByPosition(0);

    /// Flatten the tuple
    assert(isTuple(col_with_type.type));
    auto col_tuple = checkAndGetColumn<ColumnTuple>(col_with_type.column.get());

    if (wstart_pos >= 0)
    {
        auto & wstart = block.getByPosition(wstart_pos);
        wstart.column = col_tuple->getColumnPtr(0);
    }

    if (wend_pos >= 0)
    {
        auto & wend = block.getByPosition(wend_pos);
        wend.column = col_tuple->getColumnPtr(1);
    }
}

void StreamingWindowAssignmentBlockInputStream::assignHopWindow(Block & block, Block & expr_block)
{
    auto & col_with_type = expr_block.getByPosition(0);

    assert(isTuple(col_with_type.type));
    auto col_tuple = checkAndGetColumn<ColumnTuple>(col_with_type.column.get());

    if (wstart_pos >= 0)
    {
        /// const ColumnArray & src = assert_cast<const ColumnArray &>(*wstart_result);
        auto src_wstarts = checkAndGetColumn<ColumnArray>(col_tuple->getColumnPtr(0).get());
        const auto & offsets  = src_wstarts->getOffsets();
        for (auto & column_with_type : block)
            column_with_type.column = column_with_type.column->replicate(offsets);

        auto & target_wstarts = block.getByPosition(wstart_pos);

        const auto & src_wstarts_data = src_wstarts->getDataPtr();
        assert(target_wstarts.column->getDataType() == src_wstarts_data->getDataType());
        assert(target_wstarts.column->size() == src_wstarts_data->size());

        target_wstarts.column = src_wstarts_data;
    }

    if (wend_pos >= 0)
    {
        auto src_wends = checkAndGetColumn<ColumnArray>(col_tuple->getColumnPtr(1).get());

        if (wstart_pos < 0)
        {
            /// The block is not replicated yet
            const auto & offsets  = src_wends->getOffsets();
            for (auto & column_with_type : block)
                column_with_type.column = column_with_type.column->replicate(offsets);
        }

        auto & target_wends = block.getByPosition(wend_pos);

        const auto & src_wends_data = src_wends->getDataPtr();
        assert(target_wends.column->getDataType() == src_wends_data->getDataType());
        assert(target_wends.column->size() == src_wends_data->size());

        target_wends.column = src_wends_data;
    }
}

void StreamingWindowAssignmentBlockInputStream::calculateColumns(const Names & column_names)
{
    expr_column_positions.reserve(func_desc->input_columns.size());

    auto input_begin = func_desc->input_columns.begin();
    auto input_end = func_desc->input_columns.end();

    for (size_t i = 0; i < column_names.size(); ++i)
    {
        if (column_names[i] == "wstart")
            wstart_pos = i;

        if (column_names[i] == "wend")
            wend_pos = i;

        if (std::find(input_begin, input_end, column_names[i]) != input_end)
            expr_column_positions.push_back(i);
    }
}

void StreamingWindowAssignmentBlockInputStream::cancel(bool kill)
{
    input->cancel(kill);
    IBlockInputStream::cancel(kill);
}
}
