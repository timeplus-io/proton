#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
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
        const SelectQueryInfo & query_info,
        const StorageID & storage_id,
        const Names & column_names,
        ContextPtr context_);

    ~StreamingWindowAssignmentBlockInputStream() override = default;

    String getName() const override { return "StreamingWindowAssignmentBlockInputStream"; }

    Block getHeader() const override { return input->getHeader(); }

    void readPrefix() override { input->readPrefix(); }

    void cancel(bool kill) override;

private:
    Block readImpl() override;
    void assignWindow(Block & block);

private:
    BlockInputStreamPtr input;
    ContextPtr context;

    bool require_wstart = false;
    bool require_wend = false;
    String func_name;

    ExpressionActionsPtr streaming_win_expr;
};
}
