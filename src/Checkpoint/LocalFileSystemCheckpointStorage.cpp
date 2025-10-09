#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/LocalFileSystemCheckpointStorage.h>

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
        /// layout: [version] [uuid]
        writeVarUInt(1, *file_buf);
        /// a readable UUID of current epoch
        chassert(ckpt_ctx->request_ctx);
        writeBinary(ckpt_ctx->request_ctx->uuid, *file_buf);

        file_buf->finalize();
        LOG_INFO(
            logger, "Pre-committed checkpoint epoch={} uuid={} for query={}", ckpt_ctx->epoch, ckpt_ctx->request_ctx->uuid, ckpt_ctx->qid);
    }
}

std::optional<UUID> LocalFileSystemCheckpointStorage::tryGetPreCommittedUUID(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    auto pre_committed_file = ckpt_ctx->checkpointDir() / PRE_COMMITTED_FILE;
    if (!disk->exists(pre_committed_file))
        return {};

    auto file_buf = disk->readFile(pre_committed_file);
    /// layout: [version] [uuid]
    VersionType version;
    readVarUInt(version, *file_buf);
    chassert(version == 1);

    UUID pre_committed_uuid;
    readBinary(pre_committed_uuid, *file_buf);
    return pre_committed_uuid;
}

bool LocalFileSystemCheckpointStorage::preCommitted(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    auto pre_committed_file = ckpt_ctx->checkpointDir() / PRE_COMMITTED_FILE;
    return disk->exists(pre_committed_file);
}
}
