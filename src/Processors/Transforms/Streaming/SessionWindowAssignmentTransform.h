#pragma once

#include <Processors/Transforms/Streaming/WindowAssignmentTransform.h>

namespace DB
{
namespace Streaming
{
class SessionWindowAssignmentTransform final : public WindowAssignmentTransform
{
public:
    SessionWindowAssignmentTransform(const Block & input_header, const Block & output_header, WindowParamsPtr window_params_);

    ~SessionWindowAssignmentTransform() override = default;

    String getName() const override { return "StreamingSessionWindowAssignmentTransform"; }

private:
    void assignWindow(Columns & columns) const override;

private:
    SessionWindowParams & params;
};
}
}
