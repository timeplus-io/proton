#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/CheckpointRequestContext.h>

#include <Common/Exception.h>

namespace DB
{
void CheckpointRequestContext::addFinishCallbackNoExcept(std::function<void(CheckpointContextPtr)> callback_noexcept)
{
    std::scoped_lock lock(callbacks_noexcept_mutex);
    callbacks_noexcept.emplace_back(std::move(callback_noexcept));
}

void CheckpointRequestContext::onFinish(CheckpointContextPtr ckpt_ctx) const
{
    /// Execute all callbacks registered for this checkpoint request
    /// 1) noexcept callbacks
    /// 2) callback
    /// Hold the executor holder during calling onFinish() to ensure the pipeline is not destructed
    auto executor_holder = ckpt_ctx->coordinator->getExecutorHolder(ckpt_ctx->qid);
    if (!executor_holder)
        return; /// deregistered

    {
        std::scoped_lock lock(callbacks_noexcept_mutex);
        for (const auto & callback_noexcept : callbacks_noexcept)
        {
            try
            {
                callback_noexcept(ckpt_ctx);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    if (callback)
        callback(ckpt_ctx);
}
}
