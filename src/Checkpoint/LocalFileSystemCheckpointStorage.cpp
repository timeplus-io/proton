#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/LocalFileSystemCheckpointStorage.h>

#include <Common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace
{
const std::string PRE_COMMITTED_FILE = "pre_committed";
}

void LocalFileSystemCheckpointStorage::preCommit(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    auto pre_committed_file = ckpt_ctx->checkpointDir() / PRE_COMMITTED_FILE;
    if (!disk->exists(pre_committed_file))
    {
        auto file_buf = disk->writeFile(pre_committed_file);
        file_buf->finalize();
        LOG_INFO(logger, "Pre-committed checkpoint epoch={} for query={}", ckpt_ctx->epoch, ckpt_ctx->qid);
    }
}

bool LocalFileSystemCheckpointStorage::preCommitted(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    auto pre_committed_file = ckpt_ctx->checkpointDir() / PRE_COMMITTED_FILE;
    return disk->exists(pre_committed_file);
}
}
