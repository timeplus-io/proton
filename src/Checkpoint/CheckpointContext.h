#pragma once

#include <fmt/format.h>
#include <cassert>
#include <filesystem>

#include <Checkpoint/CheckpointRequestContext.h>

namespace DB
{
class CheckpointCoordinator;

struct CheckpointContext
{
    CheckpointContext(int64_t epoch_, std::string_view qid_, CheckpointCoordinator * coordinator_, CheckpointRequestContextPtr request_ctx_ = nullptr)
        : epoch(epoch_), qid(qid_), coordinator(coordinator_), request_ctx(request_ctx_)
    {
        assert(epoch >= 0);
        assert(!qid.empty());
        assert(coordinator);
    }

    /// Checkpoint epoch / monotonically increasing
    int64_t epoch;

    /// Query ID
    std::string qid;

    CheckpointCoordinator * coordinator = nullptr;

    /// If not null, it is a checkpoint request, otherwise it is used to register or recover the query
    CheckpointRequestContextPtr request_ctx;

    std::filesystem::path checkpointDir(const std::filesystem::path & base_dir) const
    {
        /// processor checkpoint epoch starts with 1
        /// graph persistent is epoch 0
        if (epoch > 0)
            return base_dir / qid / fmt::format("{}", epoch);
        else
            return base_dir / qid;
    }

    std::filesystem::path queryCheckpointDir(const std::filesystem::path & base_dir) const { return base_dir / qid; }
};
}
