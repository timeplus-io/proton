#pragma once

#include <Checkpoint/CheckpointEpoch.h>
#include <Checkpoint/CheckpointRequestContext.h>
#include <Checkpoint/ExtraCheckpointContext.h>

#include <base/defines.h>
#include <base/demangle.h>
#include <Common/Exception.h>

#include <fmt/format.h>

#include <cassert>
#include <filesystem>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

class CheckpointStorage;
class CheckpointCoordinator;

struct CheckpointContext final : public std::enable_shared_from_this<CheckpointContext>
{
    CheckpointContext(std::string_view qid_, const CheckpointStorage & storage_, CheckpointCoordinator * coordinator_)
        : qid(qid_), storage(storage_), coordinator(coordinator_)
    {
        chassert(!qid.empty());
        chassert(coordinator);
    }

    bool hasExtra() const { return extra_ctx != nullptr; }
    void setExtra(ExtraCheckpointContextPtr extra_ctx_) { extra_ctx.swap(extra_ctx_); }

    template <typename ExtraCkptCtx>
    std::shared_ptr<ExtraCkptCtx> getExtra() const
    {
        auto res = std::dynamic_pointer_cast<ExtraCkptCtx>(extra_ctx);
        if (!res) [[unlikely]]
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Failed to get extra checkpoint context '{}'", demangle(typeid(ExtraCkptCtx).name()));

        return res;
    }

    template <typename ExtraCkptCtx>
    std::shared_ptr<ExtraCkptCtx> tryGetExtra() const
    {
        return std::dynamic_pointer_cast<ExtraCkptCtx>(extra_ctx);
    }

    std::filesystem::path checkpointDir() const
    {
        /// processor checkpoint epoch starts with 1
        /// graph persistent is epoch 0
        if (!epoch.empty())
            return fmt::format("{}/{}", qid, epoch);
        else
            return qid;
    }

    std::filesystem::path queryCheckpointDir() const { return qid; }

    CheckpointContextPtr cloneWithEpoch(CheckpointEpoch ckpt_epoch, CheckpointRequestContextPtr request_ctx_ = {}) const
    {
        auto new_ckpt_ctx = std::make_shared<CheckpointContext>(*this);
        new_ckpt_ctx->epoch = ckpt_epoch;
        if (request_ctx_)
            new_ckpt_ctx->request_ctx.swap(request_ctx_);
        return new_ckpt_ctx;
    }

    void registerFinishCallback(std::function<void(CheckpointContextPtr)> callback) const
    {
        if (!request_ctx)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CheckpointContext::registerFinishCallback called without request context");

        const_cast<CheckpointRequestContext *>(request_ctx.get())->addFinishCallbackNoExcept(std::move(callback));
    }

    bool isOffsetsOnly() const
    {
        return request_ctx && request_ctx->settings && request_ctx->settings->isOffsetsOnly();
    }

    /// Checkpoint epoch / monotonically increasing
    CheckpointEpoch epoch;

    std::string qid;

    const CheckpointStorage & storage;

    CheckpointCoordinator * coordinator;

    ExtraCheckpointContextPtr extra_ctx;

    /// If not null, it is a checkpoint request, otherwise it is used to register or recover the query
    CheckpointRequestContextPtr request_ctx;
};
}
