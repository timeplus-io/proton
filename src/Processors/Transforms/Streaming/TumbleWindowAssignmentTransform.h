#pragma once

#include <Processors/Transforms/Streaming/WindowAssignmentTransform.h>

namespace DB
{
namespace Streaming
{
class TumbleWindowAssignmentTransform final : public WindowAssignmentTransform
{
public:
    TumbleWindowAssignmentTransform(const Block & input_header, const Block & output_header, WindowParamsPtr window_params_);

    ~TumbleWindowAssignmentTransform() override = default;

    String getName() const override { return "StreamingTumbleWindowAssignmentTransform"; }

private:
    void assignWindow(Chunk & chunk, Columns && columns, ColumnTuple && column_tuple) const override;

    TumbleWindowParams & params;
};
}
}
