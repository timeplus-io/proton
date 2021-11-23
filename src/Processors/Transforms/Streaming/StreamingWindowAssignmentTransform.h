#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StreamingFunctionDescription.h>
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

class StreamingWindowAssignmentTransform final : public ISimpleTransform
{
public:
    StreamingWindowAssignmentTransform(
        const Block & input_header, const Block & output_header, StreamingFunctionDescriptionPtr desc);

    ~StreamingWindowAssignmentTransform() override = default;

    String getName() const override { return "StreamingWindowAssignmentTransform"; }

    void transform(Chunk & chunk) override;

private:
    void assignWindow(Chunk & chunk);
    void assignTumbleWindow(Block & result, Block & expr_block);
    void assignHopWindow(Block & result, Block & expr_block);
    /// Calculate the positions of columns required by window expr
    void calculateColumns(const Block & input_header, const Block & output_header);

private:
    ContextPtr context;
    StreamingFunctionDescriptionPtr func_desc;

    Chunk chunk_header;

    std::vector<size_t> input_column_positions;
    std::vector<size_t> expr_column_positions;

    Int32 wstart_pos = -1;
    Int32 wend_pos = -1;
    String func_name;
};
}
