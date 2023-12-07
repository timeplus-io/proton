#pragma once

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
        std::scoped_lock lock(ckpt_mutex);
        assert(!ckpt_ctx);
        ckpt_ctx.swap(ckpt_ctx_);
        has_ckpt_request.store(true);
    }

    bool hasRequest() const noexcept { return has_ckpt_request; }

private:
    /// FIXME, switch to llvm-15 atomic shared_ptr
    std::atomic<bool> has_ckpt_request = false;
    std::mutex ckpt_mutex;
    CheckpointContextPtr ckpt_ctx TSA_PT_GUARDED_BY(ckpt_mutex);
};
}
