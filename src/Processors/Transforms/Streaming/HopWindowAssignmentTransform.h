#pragma once

#include <Processors/Transforms/Streaming/WindowAssignmentTransform.h>

namespace DB
{
namespace Streaming
{
class HopWindowAssignmentTransform final : public WindowAssignmentTransform
{
public:
    HopWindowAssignmentTransform(const Block & input_header, const Block & output_header, WindowParamsPtr window_params_);

    ~HopWindowAssignmentTransform() override = default;

    String getName() const override { return "HopWindowAssignmentTransform"; }

private:
    void assignWindow(Columns & columns) const override;

    HopWindowParams & params;
};
}
}
