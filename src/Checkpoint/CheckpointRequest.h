#pragma once

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointContextFwd.h>

#include <base/defines.h>

#include <atomic>
#include <cassert>
#include <mutex>

namespace DB
{
/// \CheckpointRequest applies double check locking to minimize using mutex
/// directly for better performance
class CheckpointRequest
{
public:
    /// \poll moves out the existing checkpoint context request atomically if there is any.
    CheckpointContextPtr poll() noexcept
    {
        CheckpointContextPtr new_request;
        if (has_ckpt_request)
        {
            std::scoped_lock lock(ckpt_mutex);
            new_request.swap(ckpt_ctx);
            has_ckpt_request.store(false);
        }
        return new_request;
    }

    void setCheckpointRequestCtx(CheckpointContextPtr ckpt_ctx_) noexcept
    {
        assert(!has_ckpt_request);
        /// Perform a deep copy to ensure that the checkpoint file is stored in a distinct path.
        CheckpointContextPtr new_ckpt = std::make_shared<CheckpointContext>(*ckpt_ctx_);

        {
            std::scoped_lock lock(ckpt_mutex);
            assert(!ckpt_ctx);
            ckpt_ctx.swap(new_ckpt);
            has_ckpt_request.store(true);
        }
    }

    bool hasRequest() const noexcept { return has_ckpt_request; }

private:
    /// FIXME, switch to llvm-15 atomic shared_ptr
    std::atomic<bool> has_ckpt_request = false;
    std::mutex ckpt_mutex;
    CheckpointContextPtr ckpt_ctx TSA_PT_GUARDED_BY(ckpt_mutex);
};
}
