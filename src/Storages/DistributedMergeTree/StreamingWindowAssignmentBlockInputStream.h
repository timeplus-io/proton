#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StreamingFunctionDescription.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
/**
 * StreamingWindowAssignmentBlockInputStream assigns window to rows in each block
 * and project `wstart, wend` columns
 */

struct SelectQueryInfo;
struct StorageID;

class StreamingWindowAssignmentBlockInputStream final : public IBlockInputStream
{
public:
    StreamingWindowAssignmentBlockInputStream(
        BlockInputStreamPtr input_,
        const Names & column_names,
        StreamingFunctionDescriptionPtr desc,
        ContextPtr context_);

    ~StreamingWindowAssignmentBlockInputStream() override = default;

    String getName() const override { return "StreamingWindowAssignmentBlockInputStream"; }

    Block getHeader() const override { return input->getHeader(); }

    void readPrefix() override { input->readPrefix(); }

    void cancel(bool kill) override;

private:
    Block readImpl() override;
    void assignWindow(Block & block);
    void assignTumbleWindow(Block & block, Block & expr_block);
    void assignHopWindow(Block & block, Block & expr_block);
    /// Calculate the positions of columns required by window expr
    void calculateColumns(const Names & column_names);

private:
    BlockInputStreamPtr input;
    ContextPtr context;

    std::vector<size_t> expr_column_positions;
    Int32 wstart_pos = -1;
    Int32 wend_pos = -1;
    String func_name;

    StreamingFunctionDescriptionPtr func_desc;
};
}
