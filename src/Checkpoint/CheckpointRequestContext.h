#pragma once

#include <Checkpoint/CheckpointContextFwd.h>
#include <Checkpoint/CheckpointRequestMetrics.h>
#include <Checkpoint/CheckpointSettings.h>
#include <Checkpoint/CheckpointableExecutorHolder.h>
#include <Core/UUID.h>

#include <functional>
#include <mutex>
#include <vector>

namespace DB
{
class PipelineExecutor;
struct CheckpointRequestContext
{
    /// Unique identifier for the checkpoint request
    UUID uuid = UUIDHelpers::generateV4();

    /// The executor that will be checkpointed. This executor is only active during the checkpoint trigger phase.
    CheckpointableExecutorHolder executor;

    CheckpointSettingsConstPtr settings;

    CheckpointRequestMetricsPtr metrics;

    /// Callback to be called after checkpointed
    std::function<void(CheckpointContextPtr)> callback;
    mutable std::mutex callbacks_noexcept_mutex;
    std::vector<std::function<void(CheckpointContextPtr)>> callbacks_noexcept TSA_GUARDED_BY(callbacks_noexcept_mutex);

    void addFinishCallbackNoExcept(std::function<void(CheckpointContextPtr)> callback_noexcept);
    void onFinish(CheckpointContextPtr ckpt_ctx) const;
};
using CheckpointRequestContextPtr = std::shared_ptr<CheckpointRequestContext>;
}
