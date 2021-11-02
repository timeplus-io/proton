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
    const SelectQueryInfo & query_info,
    const StorageID & storage_id,
    const Names & column_names,
    ContextPtr context_)
    : input(input_), context(context_), streaming_win_expr(*query_info.streaming_win_expr)
{
    assert(input);

    if (std::find(column_names.begin(), column_names.end(), "wstart") != column_names.end())
        require_wstart = true;

    if (std::find(column_names.begin(), column_names.end(), "wend") != column_names.end())
        require_wend = true;

    auto streaming_func_ast = query_info.syntax_analyzer_result->streaming_tables.at(storage_id)->as<ASTFunction>();
    assert(streaming_func_ast);
    func_name = streaming_func_ast->name;
}

Block StreamingWindowAssignmentBlockInputStream::readImpl()
{
    if (isCancelled())
    {
        return {};
    }

    Block block = input->read();
    if (block.rows())
    {
        assignWindow(block);
    }
    return block;
}

void StreamingWindowAssignmentBlockInputStream::assignWindow(Block & block)
{
    Block cloned = block;

    streaming_win_expr->execute(block);

    auto * col_with_type = block.findByName("____SWIN");
    if (!col_with_type)
        return;

    /// flatten the tuple
    assert(isTuple(col_with_type->type));
    auto col_tuple = checkAndGetColumn<ColumnTuple>(col_with_type->column.get());
    assert(col_tuple);

    /// FIXME, avoid block clone
    bool replicated = false;
    if (require_wstart)
    {
        auto wstart_result = col_tuple->getColumnPtr(0);
        if (func_name == "__TUMBLE")
        {
            auto * wstart = cloned.findByName("wstart");
            assert(wstart);

            wstart->column = wstart_result;
        }
        else
        {
            /// hop window
            /// const ColumnArray & src = assert_cast<const ColumnArray &>(*wstart_result);
            auto wstarts = checkAndGetColumn<ColumnArray>(wstart_result.get());
            const auto & offsets  = wstarts->getOffsets();
            for (auto & column_with_type : cloned)
                column_with_type.column = column_with_type.column->replicate(offsets);

            auto * wstart = cloned.findByName("wstart");
            assert(wstart);

            const auto & wstarts_data = wstarts->getDataPtr();
            assert(wstart->column->getDataType() == wstarts_data->getDataType());
            assert(wstart->column->size() == wstarts_data->size());

            wstart->column = wstarts_data;
            replicated = true;
        }
    }

    if (require_wend)
    {
        auto wend_result = col_tuple->getColumnPtr(1);

        if (func_name == "__TUMBLE")
        {
            auto * wend = cloned.findByName("wend");
            assert(wend);

            wend->column = wend_result;
        }
        else
        {
            /// hop window
            auto wends = checkAndGetColumn<ColumnArray>(wend_result.get());

            if (!replicated)
            {
                const auto & offsets  = wends->getOffsets();
                for (auto & column_with_type : cloned)
                    column_with_type.column = column_with_type.column->replicate(offsets);
            }

            auto * wend = cloned.findByName("wend");
            assert(wend);

            const auto & wends_data = wends->getDataPtr();
            assert(wend->column->getDataType() == wends_data->getDataType());
            assert(wend->column->size() == wends_data->size());

            wend->column = wends_data;
        }
    }

    block.swap(cloned);
}

void StreamingWindowAssignmentBlockInputStream::cancel(bool kill)
{
    input->cancel(kill);
    IBlockInputStream::cancel(kill);
}
}
