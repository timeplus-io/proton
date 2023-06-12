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
    void assignWindow(Columns & columns) const override;

    TumbleWindowParams & params;
};
}
}
