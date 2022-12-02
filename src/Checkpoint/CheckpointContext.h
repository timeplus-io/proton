#pragma once

#include <fmt/format.h>
#include <assert.h>
#include <filesystem>

namespace DB
{
class CheckpointCoordinator;

struct CheckpointContext
{
    CheckpointContext(int64_t epoch_, const std::string & qid_, CheckpointCoordinator * coordinator_)
        : epoch(epoch_), qid(qid_), coordinator(coordinator_)
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
