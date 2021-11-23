#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/ISimpleTransform.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
/**
 * TimestampTransform transform input column(s) to a DateTime or DateTime64
 */

class ColumnTuple;

class TimestampTransform final : public ISimpleTransform
{
public:
    TimestampTransform(
        const Block & input_header, const Block & output_header, ExpressionActionsPtr timestamp_expr_, const Names & input_columns_);

    ~TimestampTransform() override = default;

    String getName() const override { return "TimestampTransform"; }

    void transform(Chunk & chunk) override;

private:
    /// Calculate the positions of columns required by timestamp expr
    void calculateColumns(const Block & input_header, const Block & output_header, const Names & input_columns_);
    void transformTimestamp(Chunk & chunk);

private:
    ContextPtr context;
    ExpressionActionsPtr timestamp_expr;

    size_t timestamp_col_pos = 0;
    DataTypePtr timestamp_col_data_type;
    Chunk chunk_header;

    std::vector<size_t> input_column_positions;
    std::vector<size_t> expr_column_positions;
};
}
