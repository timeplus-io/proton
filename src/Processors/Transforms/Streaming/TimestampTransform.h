#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Streaming/StreamingFunctionDescription.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/ISimpleTransform.h>

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
        const Block & input_header, const Block & output_header, StreamingFunctionDescriptionPtr timestamp_func_desc_, bool backfill_);

    ~TimestampTransform() override = default;

    String getName() const override { return "TimestampTransform"; }

    void transform(Chunk & chunk) override;

private:
    /// Calculate the positions of columns required by timestamp expr
    void calculateColumns(const Block & input_header, const Block & output_header, const Names & input_columns_);
    void handleProcessingTimeFunc();
    void transformTimestamp(Chunk & chunk);
    void assignProcTimestamp(Chunk & chunk);

private:
    ContextPtr context;

    StreamingFunctionDescriptionPtr timestamp_func_desc;

    /// process time streaming processing
    bool backfill = false;
    bool proc_time = false;
    bool is_datetime64 = false;
    String timezone;
    /// For datetime64
    Int32 scale = 0;
    Int64 multiplier = 1;

    size_t timestamp_col_pos = 1;
    DataTypePtr timestamp_col_data_type;
    Chunk chunk_header;

    std::vector<size_t> input_column_positions;
    std::vector<size_t> expr_column_positions;
};
}
