#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/DiskCheckpointStorage.h>

#include <IO/WriteHelpers.h>
#include <base/ClockUtils.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CHECKPOINT_DOESNT_EXIST;
extern const int CHECKPOINT_ALREADY_EXISTS;
extern const int QUERY_WAS_CANCELLED;
extern const int CREATE_CHECKPOINT_FAILED;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace
{
const std::string COMMITTED_FILE = "committed";
const std::string DELETE_FILE = "delete";
}

DiskCheckpointStorage::DiskCheckpointStorage(const std::string & log_name) : CheckpointStorage(log_name)
{
}

void DiskCheckpointStorage::preCheckpoint(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    auto ckpt_dir = ckpt_ctx->checkpointDir();
    if (!disk->exists(ckpt_dir))
    {
        disk->createDirectories(ckpt_dir);
        LOG_INFO(logger, "Created directory '{}' on disk {}", ckpt_dir, disk->logName());
    }
    else
    {
        /// Exists delete-mark file
        if (disk->exists(ckpt_dir / DELETE_FILE))
        {
            LOG_INFO(
                logger,
                "Directory '{}' already exists on disk {}, but it was marked to deleted, so clear all contents",
                ckpt_dir,
                disk->logName());
            clearDirectory(disk, ckpt_dir);
        }
        /// No committed-mark file for specified epoch
        else if (ckpt_ctx->epoch > 0 && !disk->exists(ckpt_dir / COMMITTED_FILE))
        {
            LOG_INFO(
                logger,
                "Directory '{}' already exists on disk {}, but it doesn't be committed, so clear all contents",
                ckpt_dir,
                disk->logName());
            clearDirectory(disk, ckpt_dir);
        }
        /// Always clear ckpt root dir for initialization
        else if (ckpt_ctx->epoch == 0)
        {
            LOG_INFO(logger, "Directory '{}' already exists on disk {}, clear all contents for initialization", ckpt_dir, disk->logName());
            clearDirectory(disk, ckpt_dir);
        }
        else
        {
            throw Exception(ErrorCodes::CHECKPOINT_ALREADY_EXISTS, "Directory '{}' already exists on disk {}", ckpt_dir, disk->logName());
        }
    }
}

void DiskCheckpointStorage::checkpoint(const std::string & key, CheckpointPtr ckpt, CheckpointContextPtr ckpt_ctx) const
{
    auto ckpt_path = ckpt_ctx->checkpointDir() / fmt::format("{}{}", key, Checkpoint::postfix(ckpt->type()));
    auto materialized_bytes = ckpt->materializeTo(DiskPath{getDisk(ckpt_ctx), ckpt_path});

    if (ckpt_ctx->request_ctx)
        ckpt_ctx->request_ctx->metrics->total_bytes.fetch_add(materialized_bytes, std::memory_order_relaxed);
}

CheckpointPtr DiskCheckpointStorage::recover(const std::string & key, CheckpointContextPtr ckpt_ctx) const
{
    auto ckpt_type = exists(key, ckpt_ctx);
    if (!ckpt_type)
        throw Exception(
            ErrorCodes::CHECKPOINT_DOESNT_EXIST,
            "Cannot load checkpoint for query_id={}, key={}, epoch={}",
            ckpt_ctx->qid,
            key,
            ckpt_ctx->epoch);

    auto ckpt_path = ckpt_ctx->checkpointDir() / fmt::format("{}{}", key, Checkpoint::postfix(*ckpt_type));
    return Checkpoint::createFrom(*ckpt_type, DiskPath{getDisk(ckpt_ctx), ckpt_path});
}

void DiskCheckpointStorage::commit(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    auto committed_file = ckpt_ctx->checkpointDir() / COMMITTED_FILE;
    if (!disk->exists(committed_file))
    {
        auto file_buf = disk->writeFile(committed_file);
        file_buf->finalize();
        LOG_INFO(logger, "Committed checkpoint epoch={} for query={}", ckpt_ctx->epoch, ckpt_ctx->qid);
    }
}

int64_t DiskCheckpointStorage::getLastCommittedEpoch(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    fs::path ckpt_dir(ckpt_ctx->qid);

    if (!disk->exists(ckpt_dir) || disk->exists(ckpt_dir / DELETE_FILE))
        return 0;

    /// We'd like to touch the ckpt dir to extend its TTL
    if (storageType() == CheckpointStorageType::LocalFileSystem)
        disk->setLastModified(ckpt_dir, Poco::Timestamp::fromEpochTime(time(nullptr)));

    /// 1) Loop the directory to figure out largest committed checkpoint epoch
    int64_t max_epoch = 0;
    iterateDirectory(disk, ckpt_dir, [&](const fs::path & path, bool is_dir) {
        if (!is_dir)
            return;

        try
        {
            /// Two cases: 1) '.../path/1' 2) '.../path/1/'
            int64_t epoch = std::stoll(path.has_stem() ? path.stem().string() : path.parent_path().stem().string());
            /// check if `committed` exists
            if (epoch > max_epoch && disk->exists(path / COMMITTED_FILE))
                max_epoch = epoch;
        }
        catch (const std::invalid_argument & e)
        {
            LOG_WARNING(
                logger,
                "Directory '{}' was not a checkpoint epoch directory on disk {}, skip it. Epoch convert error: {}",
                path,
                disk->logName(),
                e.what());
        }
    });

    return max_epoch;
}

void DiskCheckpointStorage::remove(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    auto ckpt_dir = ckpt_ctx->queryCheckpointDir();
    if (ckpt_ctx->epoch <= 0)
    {
        /// Remove the whole ckpt dir for the query
        LOG_INFO(logger, "Remove checkpoint '{}' for query={}", ckpt_dir.c_str(), ckpt_ctx->qid);
        disk->removeRecursive(ckpt_dir);
        return;
    }

    /// Enumerate folders in checkpoint directory to get epoch ckpt folder
    iterateDirectory(disk, ckpt_dir, [&](const fs::path & path, bool is_dir) {
        if (!is_dir)
            return;

        int64_t epoch = 0;
        try
        {
            /// Two cases: '.../path/1' or '.../path/1/'
            epoch = std::stoll(path.has_stem() ? path.stem().string() : path.parent_path().stem().string());
        }
        catch (const std::invalid_argument & e)
        {
            /// Found a directory which is not an epoch number directory, delete this folder
            LOG_WARNING(logger, "Remove unknown format checkpoint epoch directory '{}' on disk {}: {}", path, disk->logName(), e.what());
            disk->removeRecursive(path);
            return;
        }

        if (epoch < ckpt_ctx->epoch)
        {
            LOG_INFO(logger, "Remove prev checkpoint '{}' on disk {}", path, disk->logName());
            disk->removeRecursive(path);
        }
    });
}

/// Loop all checkpoints and delete
/// 1) checkpoints which have delete-mark if delete_marked is true
/// 2) checkpoints which reach TTL
void DiskCheckpointStorage::removeExpired(
    uint64_t ttl_secs, bool delete_marked, std::function<bool(const std::string &)> delete_precheck) const
{
    /// FIXME: So far only local file system supports this feature
    if (storageType() != CheckpointStorageType::LocalFileSystem)
        return;

    auto disk = getDisk(/*extra_ckpt_ctx=*/nullptr);

    LOG_INFO(logger, "Scanning delete-marked and expired checkpoints in {}, ttl={}s", storageType(), ttl_secs);

    iterateDirectory(disk, "", [&](const fs::path & path, bool is_dir) {
        if (!is_dir)
            return;

        auto delete_file_mark = path / DELETE_FILE;
        if (delete_marked && disk->exists(delete_file_mark))
        {
            if (!delete_precheck || delete_precheck(path.filename().string()))
            {
                LOG_INFO(logger, "Remove checkpoint '{}' which was marked to delete", path);
                disk->removeRecursive(path);
                return;
            }
        }

        auto last_atime = disk->getLastModified(path);
        if (last_atime.isElapsed(/*ttl_microseconds=*/ttl_secs * Poco::Timestamp::resolution()))
        {
            if (!delete_precheck || delete_precheck(path.filename().string()))
            {
                LOG_INFO(logger, "Remove expired checkpoint '{}'", path);
                disk->removeRecursive(path);
            }
        }
    });
}

void DiskCheckpointStorage::removeOldCheckpoints() const
{
    /// FIXME: So far only local file system supports this feature
    if (storageType() != CheckpointStorageType::LocalFileSystem)
        return;

    auto disk = getDisk(/*extra_ckpt_ctx=*/nullptr);

    auto remove_old_epochs = [&](const fs::path & path, bool is_dir) {
        if (!is_dir)
            return;

        if (!std::string_view{path.c_str()}.starts_with(".mv-"))
        {
            LOG_INFO(logger, "Skipping non checkpoint dir: {}", path.c_str());
            return;
        }

        /// Collect all epochs and max committed epoch
        std::map<int64_t, fs::path> epochs;
        int64_t max_committed_epoch = 0;
        auto collect_epochs = [&](const fs::path & epoch_path, bool is_epoch_dir) {
            if (!is_epoch_dir)
                return;

            try
            {
                /// Two cases: '.../path/1' or '.../path/1/'
                int64_t epoch = std::stoll(epoch_path.has_stem() ? epoch_path.stem().string() : epoch_path.parent_path().stem().string());

                /// check if `committed` exists
                if (epoch > max_committed_epoch && disk->exists(epoch_path / COMMITTED_FILE))
                    max_committed_epoch = epoch;

                epochs.emplace(epoch, epoch_path);
            }
            catch (const std::invalid_argument & e)
            {
                LOG_WARNING(
                    logger,
                    "Directory '{}' was not a checkpoint epoch directory on disk {}, skip it. Epoch convert error: {}",
                    epoch_path,
                    disk->logName(),
                    e.what());
            }
        };

        /// `path` is expected to be a mv query uuid like .mv-e6637f8f-5c68-43d6-9d83-f7d6e36a31ff,
        /// scan this path to collect the epochs for this MatView
        iterateDirectory(disk, path, collect_epochs, /*recursive=*/false);

        /// Remove old epochs
        for (const auto & [epoch, epoch_path] : epochs)
        {
            if (epoch >= max_committed_epoch)
                break;

            /// If the epoch is less than max committed epoch, remove it
            if (epoch < max_committed_epoch)
            {
                LOG_INFO(logger, "Remove old checkpoint '{}' on disk {}", epoch_path, disk->logName());
                disk->removeRecursive(epoch_path);
            }
        }
    };

    /// Loop all checkpoint directories under checkpoint base dir and remove old epochs
    iterateDirectory(disk, "", remove_old_epochs, /*recursive=*/false);
}

std::optional<CheckpointType> DiskCheckpointStorage::exists(const std::string & key, CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    auto ckpt_dir = ckpt_ctx->checkpointDir();
    for (const auto & [ckpt_type, ckpt_postfix] : Checkpoint::supportedTypesAndPostfixes())
    {
        auto ckpt_path = ckpt_dir / fmt::format("{}{}", key, ckpt_postfix);
        if (disk->exists(ckpt_path))
            return ckpt_type;
    }

    return {};
}

bool DiskCheckpointStorage::checkpointDirExists(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    return disk->exists(ckpt_ctx->checkpointDir());
}

std::set<std::string> DiskCheckpointStorage::getKeyNames(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    std::set<std::string> key_names;
    auto ckpt_dir = ckpt_ctx->checkpointDir();

    iterateDirectory(disk, ckpt_dir, [&](const fs::path & path, bool /*is_dir*/) {
        /// Validate key name
        auto key_extension = path.has_extension() ? path.extension().string() : path.parent_path().extension().string();
        if (std::ranges::none_of(
                Checkpoint::supportedTypesAndPostfixes(), [&](const auto & type_postfix) { return key_extension == type_postfix.second; }))
            return;

        auto key_name = path.has_stem() ? path.stem().string() : path.parent_path().stem().string();
        if (key_name.empty())
            return;

        key_names.emplace(std::move(key_name));
    });

    return key_names;
}

std::unique_ptr<WriteBufferFromFileBase>
DiskCheckpointStorage::initKeyFileWriteBuffer(const std::string & key, CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    return disk->writeFile(ckpt_ctx->checkpointDir() / key);
}

std::unique_ptr<ReadBufferFromFileBase>
DiskCheckpointStorage::initKeyFileReadBuffer(const std::string & key, CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);
    return disk->readFile(ckpt_ctx->checkpointDir() / key);
}

uint64_t DiskCheckpointStorage::getStorageSize(CheckpointContextPtr ckpt_ctx) const
{
    auto disk = getDisk(ckpt_ctx);

    std::unordered_set<String> counted_sst_files;
    int64_t total_size = 0;
    iterateDirectory(
        disk,
        ckpt_ctx->queryCheckpointDir(),
        [&](const fs::path & path, bool is_dir) {
            if (is_dir)
                return;

            /// Only count same sst file once (e.g., there are same sst files in multiple epochs)
            if (path.extension() == ".sst")
            {
                auto [_, inserted] = counted_sst_files.emplace(path.filename());
                if (!inserted)
                    return;
            }

            total_size += disk->getFileSize(path);
        },
        /*recursive=*/true);

    return total_size;
}

PathSizes DiskCheckpointStorage::getStorageStat(CheckpointContextPtr ckpt_ctx) const
{
    return {{ckpt_ctx->queryCheckpointDir(), getStorageSize(ckpt_ctx)}};
}
}
