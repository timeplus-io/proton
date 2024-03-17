#include "LocalFilesystemCheckpoint.h"
#include "CheckpointContext.h"

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <base/ClockUtils.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_CREATE_DIRECTORY;
extern const int DIRECTORY_DOESNT_EXIST;
extern const int DIRECTORY_ALREADY_EXISTS;
extern const int QUERY_WAS_CANCELLED;
}

namespace
{
const std::string COMMITTED_FILE = "committed";
const std::string DELETE_FILE = "delete";
const std::string CKPT_POSTFIX = ".ckpt";
}

LocalFileSystemCheckpoint::LocalFileSystemCheckpoint(const std::string & base_dir_, uint16_t interval_)
    : CheckpointStorage("LocalFileSystemCheckpoint"), base_dir(base_dir_), interval(interval_)
{
    std::error_code ec;
    if (!std::filesystem::exists(base_dir, ec))
        std::filesystem::create_directories(base_dir, ec);
}

/// CheckpointCoordinator ensures there will be no overlap checkpoint epoch.
/// So it is safe to run checkpoint concurrently for different processors in a DAG.
/// Please note this routine assumes the checkpoint directory is already created / exists
void LocalFileSystemCheckpoint::checkpoint(
    VersionType version, const std::string & key, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt)
{
    assert(do_ckpt);

    auto start = MonotonicMilliseconds::now();

    /// Assume cpt_dir was created
    auto ckpt_dir = ckpt_ctx->checkpointDir(baseDirectory());
    auto ckpt_file = ckpt_dir / fmt::format("{}{}", key, CKPT_POSTFIX);

    WriteBufferFromFile file_buf(ckpt_file.string());

    /// CompressedWriteBuffer already does checksum
    CompressedWriteBuffer wb(file_buf);

    /// All checkpoint has the following common prefix layout
    /// [version][timestamp][ckpt_data]
    Int64 ts = UTCMilliseconds::now();
    writeIntBinary(version, wb);
    writeIntBinary(ts, wb);

    /// ckpt_data
    do_ckpt(wb);

    wb.next();
    file_buf.next();
    wb.finalize();

    auto end = MonotonicMilliseconds::now();

    LOG_INFO(
        logger,
        "Took {} ms to checkpoint to {}, compressed_size={}, uncompressed_size={}",
        end - start,
        ckpt_file.c_str(),
        file_buf.count(),
        wb.count());
}

void LocalFileSystemCheckpoint::commit(CheckpointContextPtr ckpt_ctx)
{
    auto committed_file = ckpt_ctx->checkpointDir(baseDirectory()) / COMMITTED_FILE;
    WriteBufferFromFile file_buf(committed_file.string());
}

int64_t LocalFileSystemCheckpoint::recover(const std::string & qid)
{
    auto ckpt_dir = baseDirectory() / qid;

    std::error_code ec;
    if (!std::filesystem::exists(ckpt_dir, ec))
        throw Exception(
            ErrorCodes::DIRECTORY_DOESNT_EXIST,
            "Failed to recover checkpoint since checkpoint directory '{}' doesn't exist",
            ckpt_dir.c_str());

    auto delete_file_mark = baseDirectory() / qid / DELETE_FILE;
    if (std::filesystem::exists(delete_file_mark, ec))
        throw Exception(
            ErrorCodes::QUERY_WAS_CANCELLED,
            "Failed to recover query={} because it was unsubscribed and its checkpoints is scheduled to be deleted",
            qid);

    /// We'd like to touch the ckpt dir to extend its TTL
    std::filesystem::last_write_time(ckpt_dir, std::filesystem::file_time_type::clock::now(), ec);

    /// 1) Loop the directory to figure out largest committed checkpoint epoch and also uncommitted ckpt folder if there is any
    int64_t max_epoch = 0;
    for (const auto & dir_entry : std::filesystem::directory_iterator{ckpt_dir})
    {
        if (!dir_entry.is_directory(ec))
            continue;

        const auto & path = dir_entry.path();
        try
        {
            /// check if `committed` exists
            if (std::filesystem::exists(path / COMMITTED_FILE))
            {
                if (auto epoch = std::stoll(path.stem().string()); epoch > max_epoch)
                    max_epoch = epoch;
            }
            else
            {
                /// Found uncommitted ckpt, remove it
                LOG_WARNING(logger, "Remove uncommitted checkpoint epoch directory'{}'", path.c_str());
                std::filesystem::remove_all(path, ec);
            }
        }
        catch (const std::invalid_argument &)
        {
            /// Found a directory which is not an epoch number directory, delete this folder
            LOG_WARNING(logger, "Remove unknown format checkpoint epoch directory'{}'", path.c_str());
            std::filesystem::remove_all(path, ec);
            continue;
        }
    }
    return max_epoch;
}

void LocalFileSystemCheckpoint::recover(
    const std::string & key, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType, ReadBuffer &)> do_recover)
{
    assert(do_recover);

    auto start = MonotonicMilliseconds::now();

    auto ckpt_dir = ckpt_ctx->checkpointDir(baseDirectory());
    auto ckpt_file = ckpt_dir / fmt::format("{}{}", key, CKPT_POSTFIX);

    ReadBufferFromFile file_buf(ckpt_file.string());
    CompressedReadBuffer rb(file_buf);

    VersionType version;
    readIntBinary(version, rb);

    Int64 ts;
    readIntBinary(ts, rb);

    do_recover(version, rb);

    auto end = MonotonicMilliseconds::now();

    LOG_INFO(
        logger,
        "Took {} ms to recover from checkpoint={}, compressed_size={}, uncompressed_size={}",
        end - start,
        ckpt_file.c_str(),
        file_buf.count(),
        rb.count());
}

bool LocalFileSystemCheckpoint::markRemove(CheckpointContextPtr ckpt_ctx)
{
    if (unlikely(!std::filesystem::exists(ckpt_ctx->queryCheckpointDir(baseDirectory()))))
        return false;

    auto delete_file_mark = ckpt_ctx->checkpointDir(baseDirectory()) / DELETE_FILE;
    WriteBufferFromFile file_buf(delete_file_mark.string());
    return true;
}

void LocalFileSystemCheckpoint::remove(CheckpointContextPtr ckpt_ctx)
{
    auto ckpt_dir = ckpt_ctx->queryCheckpointDir(baseDirectory());
    if (ckpt_ctx->epoch <= 0)
    {
        /// Remove the whole ckpt dir for the query
        LOG_INFO(logger, "Remove checkpoint '{}' for query={}", ckpt_dir.c_str(), ckpt_ctx->qid);

        std::error_code ec;
        std::filesystem::remove_all(ckpt_dir, ec);
        return;
    }

    /// Enumerate folders in checkpoint directory to get epoch ckpt folder
    for (const auto & dir_entry : std::filesystem::directory_iterator{ckpt_dir})
    {
        const auto & path = dir_entry.path();
        std::error_code ec;
        if (!dir_entry.is_directory(ec))
            continue;

        int64_t epoch = 0;
        try
        {
            epoch = std::stoll(path.stem().string());
        }
        catch (const std::invalid_argument &)
        {
            /// Found a directory which is not an epoch number directory, delete this folder
            LOG_WARNING(logger, "Remove unknown format checkpoint epoch directory'{}'", path.c_str());
            std::filesystem::remove_all(path, ec);
            continue;
        }

        if (epoch < ckpt_ctx->epoch)
        {
            LOG_INFO(logger, "Remove prev checkpoint '{}'", path.c_str());
            std::filesystem::remove_all(path, ec);
        }
    }
}

/// Loop all checkpoints and delete
/// 1) checkpoints which have delete-mark if delete_marked is true
/// 2) checkpoints which reach TTL
void LocalFileSystemCheckpoint::removeExpired(uint64_t ttl_secs, bool delete_marked, std::function<bool(const std::string &)> delete_precheck)
{
    LOG_INFO(
        logger, "Scanning delete-marked and expired checkpoints in checkpoint directory={}, ttl={}s", baseDirectory().c_str(), ttl_secs);

    for (const auto & dir_entry : std::filesystem::directory_iterator{baseDirectory()})
    {
        std::error_code ec;
        if (!dir_entry.is_directory(ec))
            continue;

        const auto & path = dir_entry.path();
        auto delete_file_mark = path / DELETE_FILE;
        if (delete_marked && std::filesystem::exists(delete_file_mark))
        {
            if (!delete_precheck || delete_precheck(path.filename().string()))
            {
                LOG_INFO(logger, "Remove checkpoint '{}' which was marked to delete", path.c_str());
                std::filesystem::remove_all(path, ec);
            }
        }
        else
        {
            auto last_atime = std::filesystem::last_write_time(path, ec);
            auto now = std::filesystem::file_time_type::clock::now();
            if (last_atime + std::chrono::seconds(ttl_secs) <= now)
            {
                if (!delete_precheck || delete_precheck(path.filename().string()))
                {
                    LOG_INFO(
                        logger,
                        "Remove checkpoint '{}' which have expired TTL, last_accessed={}, now={}",
                        path.c_str(),
                        std::chrono::duration_cast<std::chrono::seconds>(last_atime.time_since_epoch()).count(),
                        std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());

                    std::filesystem::remove_all(path, ec);
                }
            }
        }
    }
}

bool LocalFileSystemCheckpoint::exists(const std::string & key, CheckpointContextPtr ckpt_ctx) const
{
    auto ckpt_file = ckpt_ctx->checkpointDir(baseDirectory()) / fmt::format("{}{}", key, CKPT_POSTFIX);

    std::error_code ec;
    return std::filesystem::exists(ckpt_file, ec);
}

void LocalFileSystemCheckpoint::preCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    if (unlikely(ckpt_ctx->epoch > 0 && !std::filesystem::exists(ckpt_ctx->queryCheckpointDir(baseDirectory()))))
        throw Exception(
            ErrorCodes::DIRECTORY_DOESNT_EXIST,
            "The query checkpoint dir '{}' doesn't exist",
            ckpt_ctx->queryCheckpointDir(baseDirectory()).string());

    auto dir = ckpt_ctx->checkpointDir(baseDirectory());
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec))
    {
        auto success = std::filesystem::create_directories(dir, ec);
        if (!success)
        {
            if (std::filesystem::exists(dir, ec))
                /// Somebody else created it
                LOG_WARNING(logger, "Directory '{}' already exists, somebody created it", dir.c_str());
            else
                throw Exception(
                    ErrorCodes::CANNOT_CREATE_DIRECTORY, "Failed to create directory '{}', error={}", dir.c_str(), ec.message());
        }
    }
    else
    {
        /// Exists delete-mark file
        if (std::filesystem::exists(dir / DELETE_FILE, ec))
        {
            LOG_WARNING(logger, "Directory '{}' already exists, but it was marked to deleted, so clear all contents", dir.c_str());
            std::filesystem::directory_iterator dir_it(dir);
            for (auto & entry : dir_it)
                std::filesystem::remove_all(entry.path(), ec);
        }
        /// No committed-mark file
        else if (!std::filesystem::exists(dir / COMMITTED_FILE, ec))
        {
            LOG_WARNING(logger, "Directory '{}' already exists, but it doesn't be committed, so clear all contents", dir.c_str());
            std::filesystem::directory_iterator dir_it(dir);
            for (auto & entry : dir_it)
                std::filesystem::remove_all(entry.path(), ec);
        }
        else
            throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Directory '{}' already exists", dir.c_str());
    }
}

}
