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
 * and project `wstart, wend` columns
 */

class StreamingWindowAssignmentTransform final : public ISimpleTransform
{
public:
    StreamingWindowAssignmentTransform(
        const Block & header, const Names & column_names, StreamingFunctionDescriptionPtr desc, ContextPtr context_);

    ~StreamingWindowAssignmentTransform() override = default;

    String getName() const override { return "StreamingWindowAssignmentTransform"; }

    void transform(Chunk & chunk) override;

private:
    void assignWindow(Chunk & chunk);
    void assignTumbleWindow(Block & block, Block & expr_block);
    void assignHopWindow(Block & block, Block & expr_block);
    /// Calculate the positions of columns required by window expr
    void calculateColumns(const Names & column_names);

private:
    ContextPtr context;
    StreamingFunctionDescriptionPtr func_desc;

    std::vector<size_t> expr_column_positions;
    Int32 wstart_pos = -1;
    Int32 wend_pos = -1;
    String func_name;
};
}
