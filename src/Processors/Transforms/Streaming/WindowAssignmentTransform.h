#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/Streaming/WindowCommon.h>
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
class WindowAssignmentTransform : public ISimpleTransform
{
public:
    WindowAssignmentTransform(const Block & input_header, const Block & output_header, WindowParamsPtr window_params_, ProcessorID pid_);

    ~WindowAssignmentTransform() override = default;

    void transform(Chunk & chunk) override;

    using WindowAssignmentFunc = std::function<void(Chunk &, const Columns &, ColumnTuple &)>;

private:
    /// Calculate the positions of columns required by window expr
    void calculateColumns(const Block & input_header, const Block & output_header);
    virtual void assignWindow(Chunk & chunk, Columns && columns, ColumnTuple && column_tuple) const = 0;

protected:
    Chunk chunk_header;

    WindowParamsPtr window_params;

    std::vector<size_t> expr_column_positions;
    std::vector<ssize_t> output_column_positions;
};
}
}
