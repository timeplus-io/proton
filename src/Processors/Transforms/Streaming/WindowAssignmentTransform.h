#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Streaming/FunctionDescription.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{
/**
 * StreamingWindowAssignmentBlockInputStream assigns window to rows in each block
 * and project `window_start, window_end` columns. It assumes the input stream doesn't
 * have these 2 columns
 */

class ColumnTuple;

namespace Streaming
{
class WindowAssignmentTransform final : public ISimpleTransform
{
public:
    WindowAssignmentTransform(const Block & input_header, const Block & output_header, FunctionDescriptionPtr desc);

    ~WindowAssignmentTransform() override = default;

    String getName() const override { return "StreamingWindowAssignmentTransform"; }

    void transform(Chunk & chunk) override;

private:
    void assignWindow(Chunk & chunk);
    void assignTumbleWindow(Block & result, const ColumnTuple * col_tuple);
    void assignHopWindow(Block & result, const ColumnTuple * col_tuple);
    void assignSessionWindow(Block & result);
    /// Calculate the positions of columns required by window expr
    void calculateColumns(const Block & input_header, const Block & output_header);

private:
    ContextPtr context;
    FunctionDescriptionPtr func_desc;

    Chunk chunk_header;

    std::vector<size_t> input_column_positions;
    std::vector<size_t> expr_column_positions;

    Int32 wstart_pos = -1;
    Int32 wend_pos = -1;
    String func_name;
};
}
}
