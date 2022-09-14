#pragma once

#include "SimpleFunctionExecutor.h"

#include <Interpreters/WindowDescription.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{
namespace Streaming
{
using WindowFunctionExecutor = typename Substream::SimpleFunctionExecutor<>;
using WindowFunctionWorkspace = typename WindowFunctionExecutor::Workspace;

/*
 * Computes several window functions that share the same window.
 * two function categories: 'aggregate function' and 'pure function'
 */
class WindowTransform final : public ISimpleTransform
{
public:
    WindowTransform(
        const Block & input_header_,
        const Block & output_header_,
        const WindowDescription & window_description_,
        const std::vector<WindowFunctionDescription> & functions);

    String getName() const override { return "StreamingWindowTransform"; }

    void transform(Chunk &) override;

public:
    std::unique_ptr<WindowFunctionExecutor> executor;
    Chunk empty_chunk;
};

}
}
