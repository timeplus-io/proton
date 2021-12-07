#include "TimestampTransform.h"

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/ProtonCommon.h>
#include <Common/timeScale.h>

namespace DB
{
TimestampTransform::TimestampTransform(
    const Block & input_header, const Block & output_header, StreamingFunctionDescriptionPtr timestamp_func_desc_)
    : ISimpleTransform(input_header, output_header, false)
    , timestamp_func_desc(std::move(timestamp_func_desc_))
    , chunk_header(output_header.getColumns(), 0)
{
    assert(timestamp_func_desc);

    calculateColumns(input_header, output_header, timestamp_func_desc->input_columns);
    handleProcessingTimeFunc();
}

void TimestampTransform::transform(Chunk & chunk)
{
    if (chunk.hasRows())
    {
        if (proc_time)
            assignProcTimestamp(chunk);
        else
            transformTimestamp(chunk);
    }
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

    timestamp_func_desc->expr->execute(expr_block);

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
        result.getByPosition(col_pos) = std::move(block.getByPosition(input_column_positions[col_pos - 1]));

    chunk.setColumns(result.getColumns(), result.rows());
}

void TimestampTransform::assignProcTimestamp(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    ColumnPtr column;
    if (is_datetime64)
        /// Call now64(scale, timezone)
        column = timestamp_col_data_type->createColumnConst(num_rows, nowSubsecond(scale));
    else
        /// Call now(timezone)
        column = timestamp_col_data_type->createColumnConst(num_rows, static_cast<UInt64>(time(nullptr)));

    auto time_column = column->convertToFullColumnIfConst();

    /// Input: col1, col2
    /// Output: col1, col2, _tp_ts and _tp_ts can be in any position
    /// Add time_column to timestamp_col_pos in chunk

    if (timestamp_col_pos == chunk.getNumColumns())
        /// ___ts is the last column
        chunk.addColumn(std::move(time_column));
    else
    {
        Columns columns(chunk.detachColumns());
        columns.emplace(columns.begin() + timestamp_col_pos, std::move(time_column));
        chunk.setColumns(columns, num_rows);
    }
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

void TimestampTransform::handleProcessingTimeFunc()
{
    if (!timestamp_func_desc->input_columns.empty())
        return;

    auto * func_node = timestamp_func_desc->func_ast->as<ASTFunction>();
    assert(func_node);
    assert(func_node->name == "now" || func_node->name == "now64");

    auto get_timezone = [&](size_t index) {
        /// arguments is an ASTExpressionList
        if (auto * literal = func_node->arguments->children[index]->as<ASTLiteral>())
            timezone = literal->value.safeGet<String>();
        else
            throw Exception(
                "Only support literal timezone argument for now/now64() transformed timestamp column", ErrorCodes::BAD_ARGUMENTS);
    };

    /// Parse function arguments
    if (func_node->name == "now")
    {
        is_datetime64 = false;

        /// We can basically assert timestamp_col_data_type is DataTypeDateTime
        if (const auto * type = typeid_cast<const DataTypeDateTime *>(timestamp_col_data_type.get()))
        {
            timezone = type->getTimeZone().getTimeZone();
        }
        else
        {
            /// now() or now(timezone). `timezone` is literal
            if (func_node->arguments && !func_node->arguments->children.empty())
                get_timezone(0);
        }
    }
    else
    {
        assert(func_node->name == "now64");
        is_datetime64 = true;

        if (const auto * type = typeid_cast<const DataTypeDateTime64 *>(timestamp_col_data_type.get()))
        {
            scale = type->getScale();
            timezone = type->getTimeZone().getTimeZone();
        }
        else
        {
            /// now64(), now64(scale) or now64(scale, timezone). `scale` and `timezone` are literals
            if (!func_node->arguments)
            {
                /// By default, it is millisecond resolution
                scale = 3;
            }
            else
            {
                auto get_scale = [&]() {
                    if (auto * literal = func_node->arguments->children[0]->as<ASTLiteral>())
                        scale = literal->value.safeGet<UInt64>();
                    else
                        throw Exception(
                            "Only support integral literal scale argument for now64() transformed timestamp column",
                            ErrorCodes::BAD_ARGUMENTS);
                };

                /// arguments is an ASTExpressionList
                if (func_node->arguments->children.size() == 1)
                {
                    get_scale();
                }
                else if (func_node->arguments->children.size() == 2)
                {
                    get_scale();
                    get_timezone(1);
                }
                else
                    scale = 3;
            }
        }
    }

    proc_time = true;
}
}
