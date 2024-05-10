#pragma once

#include <Checkpoint/CheckpointContextFwd.h>

namespace DB
{
class PipelineExecutor;

struct CheckpointRequestContext
{
    /// Executor to be checkpointed
    std::weak_ptr<PipelineExecutor> executor;

    /// Callback to be called after checkpointed
    std::function<void(CheckpointContextPtr)> callback;
};
using CheckpointRequestContextPtr = std::shared_ptr<CheckpointRequestContext>;
}
