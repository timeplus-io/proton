#include "TimestampTransform.h"

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>
#include <Common/intExp.h>
#include <Common/timeScale.h>

#include <cmath>

namespace DB
{
namespace Streaming
{
TimestampTransform::TimestampTransform(
    const Block & input_header, const Block & output_header, TimestampFunctionDescriptionPtr timestamp_func_desc_, bool backfill_)
    : ISimpleTransform(input_header, output_header, false, ProcessorID::TimestampTransformID)
    , timestamp_func_desc(std::move(timestamp_func_desc_))
    , backfill(backfill_)
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
        /// For now, we support `__streaming_now` and `__streaming_now64` to get processing time.
        /// So only handle specially in case `proc_time && backfill`
        if (unlikely(backfill && proc_time))
        {
            if (!backfillProcTimestamp(chunk))
                transformTimestamp(chunk); /// No backfill, then switch common transform
        }
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

    /// In case `__streaming_now()`, the expr_block is empty, so we must pass in rows
    auto num_rows = block.rows();
    timestamp_func_desc->expr->execute(expr_block, num_rows);
    expr_block.getByPosition(0).column = expr_block.getByPosition(0).column->convertToFullColumnIfConst();

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

bool TimestampTransform::backfillProcTimestamp(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    ColumnPtr column;

    if (unlikely(proc_time && backfill))
    {
        /// When we are backfilling, we use log append time as proc time until we catch up to the latest stream
        /// We assume the timestamp is in sync between streaming store server and proton server
        auto chunk_ctx = chunk.getChunkContext();
        if (chunk_ctx && chunk_ctx->hasAppendTime())
        {
            auto append_time = chunk_ctx->getAppendTime();
            auto delta = UTCMilliseconds::now() - append_time;
            // Heuristic
            if (delta < 1000)
            {
                /// Disable backfill since we caught up. Fall through to assign real proc time
                backfill = false;
            }
            else
            {
                /// Use append time as proc time. append time is milliseconds
                Int64 seconds = append_time / 1000;
                Int64 fractional = append_time % 1000;
                if (scale > 3)
                    fractional *= multiplier;
                else if (scale < 3)
                    fractional /= multiplier;

                if (is_datetime64)
                {
                    DecimalUtils::DecimalComponents<DateTime64> components{seconds, fractional};
                    column = timestamp_col_data_type->createColumnConst(
                        num_rows, DecimalField(DecimalUtils::decimalFromComponents<DateTime64>(components, scale), scale));
                }
                else
                    column = timestamp_col_data_type->createColumnConst(num_rows, static_cast<UInt64>(seconds));
            }
        }
        else
            /// if no append time, disable backfill mode immediately
            backfill = false;
    }

    if (!column)
        return false;

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
    return true;
}

void TimestampTransform::calculateColumns(const Block & input_header, const Block & output_header, const Names & input_columns)
{
    expr_column_positions.reserve(input_columns.size());

    /// Calculate the position of STREAMING_TIME_ALIAS column in resulting chunk
    size_t pos = 0;
    for (const auto & col_with_type : output_header)
    {
        if (col_with_type.name == ProtonConsts::STREAMING_TIMESTAMP_ALIAS)
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
    if (!timestamp_func_desc->is_now_func)
        return;

    auto * func_node = timestamp_func_desc->func_ast->as<ASTFunction>();
    assert(func_node);
    assert(func_node->name == "__streaming_now" || func_node->name == "__streaming_now64");

    auto get_timezone = [&](size_t index) {
        /// arguments is an ASTExpressionList
        if (auto * literal = func_node->arguments->children[index]->as<ASTLiteral>())
            timezone = literal->value.safeGet<String>();
        else
            throw Exception(
                "Only support literal timezone argument for now/now64() transformed timestamp column", ErrorCodes::BAD_ARGUMENTS);
    };

    /// Parse function arguments
    if (func_node->name == "__streaming_now")
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
        assert(func_node->name == "__streaming_now64");
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
                        scale = static_cast<Int32>(literal->value.safeGet<UInt64>());
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

    multiplier = intExp10(std::abs(scale - 3));

    proc_time = true;
}
}
}
