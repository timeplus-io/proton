#include <Checkpoint/RocksCheckpoint.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>

#include <rocksdb/utilities/checkpoint.h>

namespace CurrentMetrics
{
extern const Metric IDiskCopierThreads;
extern const Metric IDiskCopierThreadsActive;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CREATE_CHECKPOINT_FAILED;
}

bool RocksCheckpoint::enableIncremental(CheckpointConstPtr prev_ckpt)
{
    chassert(prev_ckpt);
    if (prev_ckpt->type() == CheckpointType::Rocks)
    {
        auto prev_rocks_ckpt = std::static_pointer_cast<const RocksCheckpoint>(prev_ckpt);
        if (auto * materialized_path = std::get_if<MaterializedEntity>(&prev_rocks_ckpt->data))
        {
            prev_ckpt_path = *materialized_path;

            iterateDirectory(prev_ckpt_path->disk, prev_ckpt_path->path, [&](const fs::path & path, bool is_dir) {
                if (is_dir)
                    return;

                /// Only skip existed sst files of previous checkpoint
                if (path.extension() == ".sst")
                    old_sst_files.insert(path.filename().string());
            });

            return true;
        }
    }

    LOG_WARNING(
        logger,
        "Cannot enable incremental rocks ckpt, previous ckpt is invalid or not materialized: type={}, version={}",
        prev_ckpt->type(),
        prev_ckpt->getVersion());
    return false;
}

RocksCheckpoint::RocksCheckpoint(VersionType version_, RocksPtr rocks)
{
    version = version_;
    data.emplace<Entity>(rocks);
}

RocksCheckpoint::RocksCheckpoint(VersionType version_, std::unique_ptr<ReadBuffer> serialized_data, bool compressed)
{
    version = version_;
    data.emplace<SerializedEntity>(std::move(serialized_data), compressed);
}

RocksCheckpoint::RocksCheckpoint(const DiskPath & ckpt_path)
{
    auto rb = ckpt_path.disk->readFile(fs::path(ckpt_path.path) / "VERSION");
    readVarUInt(version, *rb);
    data.emplace<MaterializedEntity>(ckpt_path);
}

size_t RocksCheckpoint::materializeTo(const DiskPath & ckpt_path)
{
    /// Invalidate disposable entity after used
    SCOPE_EXIT({ data = std::variant<Entity, SerializedEntity, MaterializedEntity>(); });

    if (data.valueless_by_exception()) [[unlikely]]
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot materialize an empty checkpoint");

    Stopwatch stopwatch;

    if (ckpt_path.disk->exists(ckpt_path.path))
    {
        LOG_WARNING(logger, "Rocks directory already exists, clear all contents: {}", ckpt_path.logName());
        ckpt_path.disk->removeRecursive(ckpt_path.path);
    }

    size_t materialized_bytes = 0;
    /// There are three branches:
    /// 1) If current ckpt data is in rocks, we need to use rocksdb::Checkpoint to create a new checkpoint in \ckpt_path
    /// 2) If current ckpt data is serialized, we need to deserialize it to \ckpt_path
    /// 3) If current ckpt data has been materialized to \materialized_path, we can copy it to \ckpt_path directly
    if (auto * entity = std::get_if<Entity>(&data))
    {
        /// If not a local disk, materialize rocks entity to a local temporary directory first,
        /// Then copy to the target disk
        if (!std::dynamic_pointer_cast<DiskLocal>(ckpt_path.disk))
        {
            TemporaryDiskPath tmp_rocks_dir(fmt::format("{}___materialized_tmp", entity->rocks->getDB()->GetName()));
            materialized_bytes += materializeTo(tmp_rocks_dir);
            materialized_bytes += copyRocks(tmp_rocks_dir, ckpt_path);
            return materialized_bytes;
        }

        rocksdb::Checkpoint * raw_checkpoint = nullptr;
        auto status = rocksdb::Checkpoint::Create(entity->rocks->getDB(), &raw_checkpoint);
        if (!status.ok())
            throw Exception(ErrorCodes::CREATE_CHECKPOINT_FAILED, "Failed to materialize rocks ckpt: {}", status.ToString());

        std::unique_ptr<rocksdb::Checkpoint> checkpoint(raw_checkpoint);

        auto rocks_dir = fs::path(ckpt_path.disk->getPath()) / ckpt_path.path; /// local disk full path
        status = checkpoint->CreateCheckpoint(rocks_dir);
        if (!status.ok())
            throw Exception(ErrorCodes::CREATE_CHECKPOINT_FAILED, "Failed to materialize rocks ckpt: {}", status.ToString());
    }
    else if (auto * serialized_entity = std::get_if<SerializedEntity>(&data))
    {
        materialized_bytes += deserializeEntity(*serialized_entity, ckpt_path);
    }
    else if (auto * materialized_path = std::get_if<MaterializedEntity>(&data))
    {
        materialized_bytes += copyRocks(*materialized_path, ckpt_path);
    }

    auto file_buf = ckpt_path.disk->writeFile(fs::path(ckpt_path.path) / "VERSION");
    writeVarUInt(version, *file_buf);
    file_buf->finalize();
    materialized_bytes += file_buf->count();

    LOG_INFO(
        logger,
        "Took {} ms to materialized {}rocks ckpt to {}, materialized_bytes={}",
        stopwatch.elapsedMilliseconds(),
        isIncremental() ? "incremental " : "",
        ckpt_path.logName(),
        materialized_bytes);
    return materialized_bytes;
}

void RocksCheckpoint::serializeEntity(WriteBuffer & wb, bool compress)
{
    /// Invalidate disposable entity after used
    SCOPE_EXIT({ data = std::variant<Entity, SerializedEntity, MaterializedEntity>(); });

    if (data.valueless_by_exception()) [[unlikely]]
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize an empty checkpoint");

    Stopwatch stopwatch;

    auto prev_size = wb.count();
    if (auto * serialized_entity = std::get_if<SerializedEntity>(&data))
    {
        if (compress == serialized_entity->compressed)
        {
            copyData(*serialized_entity->buffer, wb);
        }
        else if (compress)
        {
            CompressedWriteBuffer compressed_wb(wb);
            copyData(*serialized_entity->buffer, compressed_wb);
            compressed_wb.finalize();
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert a compressed buf to an uncompressed buffer");
        }
    }
    else
    {
        std::optional<TemporaryDiskPath> tmp_rocks_dir;
        auto * materialized_path = std::get_if<MaterializedEntity>(&data);
        if (!materialized_path)
        {
            /// Materialize rocks entity to a temporary directory before serialization:
            /// 1) Flush all memtables to sst files
            /// 2) Filter out unnecessary files
            tmp_rocks_dir.emplace(fmt::format("{}___serialized_tmp", std::get<Entity>(data).rocks->getDB()->GetName()));
            materializeTo(*tmp_rocks_dir);
            materialized_path = &*tmp_rocks_dir;
        }

        /// Serialize rocks dir
        /// Layout: (file_name + file_size + file_data) ... + empty_file_name
        WriteBuffer * out;
        std::unique_ptr<CompressedWriteBuffer> compressed_wb;
        if (compress)
        {
            compressed_wb = std::make_unique<CompressedWriteBuffer>(wb);
            out = compressed_wb.get();
        }
        else
        {
            out = &wb;
        }

        size_t serialized_files_count = 0;
        size_t serialized_files_bytes = 0;
        size_t skipped_files_count = 0;
        size_t skipped_files_bytes = 0;
        iterateDirectory(materialized_path->disk, materialized_path->path, [&](const fs::path & path, bool is_dir) {
            if (is_dir)
                return;

            auto file_name = path.filename();
            auto file_size = materialized_path->disk->getFileSize(path);

            writeStringBinary(file_name.string(), *out);

            if (old_sst_files.contains(file_name.string()))
            {
                writeVarUInt(0, *out); /// zero means skip this file
                ++skipped_files_count;
                skipped_files_bytes += file_size;
                return;
            }

            writeVarUInt(file_size, *out);

            auto file_buf = materialized_path->disk->readFile(path);
            copyData(*file_buf, *out);

            ++serialized_files_count;
            serialized_files_bytes += file_size;
        });

        /// End with an empty file name
        writeStringBinary("", *out);

        if (compressed_wb)
            compressed_wb->finalize();

        LOG_INFO(
            logger,
            "Serialized files count: {}, bytes: {}; Skipped files count: {}, bytes: {}",
            serialized_files_count,
            serialized_files_bytes,
            skipped_files_count,
            skipped_files_bytes);
    }

    LOG_INFO(
        logger,
        "Took {} ms to serialize {}rocks ckpt, serialized_bytes={}",
        stopwatch.elapsedMilliseconds(),
        isIncremental() ? "incremental " : "",
        wb.count() - prev_size);
}

size_t RocksCheckpoint::deserializeEntity(SerializedEntity & serialized_entity, const DiskPath & ckpt_path)
{
    Stopwatch stopwatch;

    chassert(!ckpt_path.disk->exists(ckpt_path.path));
    ckpt_path.disk->createDirectories(ckpt_path.path);

    ReadBuffer * rb;
    std::unique_ptr<CompressedReadBuffer> compressed_rb;
    if (serialized_entity.compressed)
    {
        compressed_rb = std::make_unique<CompressedReadBuffer>(*serialized_entity.buffer);
        rb = compressed_rb.get();
    }
    else
    {
        rb = serialized_entity.buffer.get();
    }

    /// Deserialize rocks files
    /// Layout: (file_name + file_size + file_data) ... + empty_file_name
    size_t deserialized_files_count = 0;
    size_t deserialized_files_bytes = 0;
    fs::path rocks_dir(ckpt_path.path);

    std::unordered_set<std::string> skipped_sst_files;
    while (!rb->eof())
    {
        String file_name;
        readStringBinary(file_name, *rb);

        /// End with an empty file path
        if (file_name.empty())
            break;

        size_t file_size;
        readVarUInt(file_size, *rb);

        /// Zero file size indicates skipped SST file in incremental checkpoint
        if (file_size == 0)
        {
            if (!isIncremental())
                throw Exception(
                    ErrorCodes::CREATE_CHECKPOINT_FAILED,
                    "Found a skipped old file '{}' while deserializing non-incremental rocks checkpoint",
                    file_name);

            skipped_sst_files.emplace(std::move(file_name));
            continue;
        }

        {
            auto file_buf = ckpt_path.disk->writeFile(rocks_dir / file_name);
            copyData(*rb, *file_buf, file_size);
            file_buf->finalize();
        }

        ++deserialized_files_count;
        deserialized_files_bytes += file_size;
    }

    /// Restore skipped SST files from previous checkpoint from previous checkpoint
    /// Compatibility: if no skipped SST files found in incremental checkpoint, restore all old SST files from previous checkpoint to the new one.
    if (isIncremental() && skipped_sst_files.empty())
        skipped_sst_files = old_sst_files;

    std::atomic<size_t> copied_files_count = 0;
    std::atomic<size_t> copied_files_bytes = 0;
    size_t linked_files_count = 0;
    size_t linked_files_bytes = 0;
    if (!skipped_sst_files.empty())
    {
        chassert(prev_ckpt_path);

        std::vector<std::string_view> hardlink_failed_sst_files;
        for (const auto & old_sst_file : skipped_sst_files)
        {
            DiskPath prev_sst_disk_path{prev_ckpt_path->disk, prev_ckpt_path->path / old_sst_file};
            DiskPath to_sst_disk_path{ckpt_path.disk, ckpt_path.path / old_sst_file};

            if (copyDiskPathViaHardLinking(prev_sst_disk_path, to_sst_disk_path))
            {
                ++linked_files_count;
                linked_files_bytes += prev_sst_disk_path.disk->getFileSize(prev_sst_disk_path.path);
            }
            else
            {
                hardlink_failed_sst_files.emplace_back(old_sst_file);
            }
        }

        /// Fallback to copy failed hard linked SST files
        if (!hardlink_failed_sst_files.empty())
        {
            ThreadPool pool(
                CurrentMetrics::IDiskCopierThreads,
                CurrentMetrics::IDiskCopierThreadsActive,
                /*pool_size=*/std::min(8ul, hardlink_failed_sst_files.size()));
            for (const auto & failed_sst_files : hardlink_failed_sst_files)
            {
                DiskPath prev_sst_disk_path{prev_ckpt_path->disk, prev_ckpt_path->path / failed_sst_files};
                DiskPath to_sst_disk_path{ckpt_path.disk, ckpt_path.path / failed_sst_files};
                pool.scheduleOrThrowOnError([&,
                                             from = std::move(prev_sst_disk_path),
                                             to = std::move(to_sst_disk_path),
                                             thread_group = CurrentThread::getGroup()]() {
                    SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););
                    if (thread_group)
                        CurrentThread::attachToGroupIfDetached(thread_group);

                    setThreadName("CkptCopier");
                    from.disk->copyFile(from.path, *to.disk, to.path);
                    copied_files_count.fetch_add(1, std::memory_order_relaxed);
                    copied_files_bytes.fetch_add(from.disk->getFileSize(from.path), std::memory_order_relaxed);
                });
            }

            pool.wait();
        }
    }

    LOG_INFO(
        logger,
        "Deserialized files count: {}, bytes: {}, Copied files count: {}, bytes: {}; Linked files count: {}, bytes: {}; Elapsed {} ms",
        deserialized_files_count,
        deserialized_files_bytes,
        copied_files_count.load(),
        copied_files_bytes.load(),
        linked_files_count,
        linked_files_bytes,
        stopwatch.elapsedMilliseconds());

    return deserialized_files_bytes + copied_files_bytes.load();
}

void RocksCheckpoint::recover(const std::filesystem::path & recovered_path)
{
    /// Invalidate disposable entity after used
    SCOPE_EXIT({ data = std::variant<Entity, SerializedEntity, MaterializedEntity>(); });

    if (auto * materialized_path = std::get_if<MaterializedEntity>(&data))
        copyRocks(*materialized_path, DiskPath(recovered_path));
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot recover an invalid checkpoint");
}

size_t RocksCheckpoint::copyRocks(const DiskPath & from, const DiskPath & to) const
{
    if (from == to)
        return 0;

    Stopwatch stopwatch;

    if (to.disk->exists(to.path))
    {
        LOG_WARNING(logger, "Rocks directory already exists, clear all contents: {}", to.logName());
        to.disk->removeRecursive(to.path);
    }

    to.disk->createDirectories(to.path);

    /// Copy rocks files in parallel
    std::atomic<size_t> copied_files_count = 0;
    std::atomic<size_t> copied_files_bytes = 0;
    size_t linked_files_count = 0;
    size_t linked_files_bytes = 0;

    ThreadPool pool(CurrentMetrics::IDiskCopierThreads, CurrentMetrics::IDiskCopierThreadsActive, /*pool_size=*/8);

    /// for example: from_disk:/path/2/rocks => to_disk:/path/2/rocks
    iterateDirectory(from.disk, from.path, [&](const fs::path & path, bool is_dir) {
        if (is_dir)
            return;

        /// Try create hard links for sst files first
        bool is_sst_file = path.extension() == ".sst";
        if (is_sst_file)
        {
            try
            {
                auto file_name = path.filename();
                /// If enabled incremental, try create hard links for old sst files:
                /// to_disk:/path/1/rocks/*.sst --(hardlink)--> to_disk:/path/2/rocks/*.sst
                if (old_sst_files.contains(file_name))
                {
                    chassert(prev_ckpt_path);
                    auto old_sst_file_path = prev_ckpt_path->path / file_name;
                    if (copyDiskPathViaHardLinking(DiskPath{to.disk, old_sst_file_path}, DiskPath(to.disk, to.path / file_name)))
                    {
                        ++linked_files_count;
                        linked_files_bytes += to.disk->getFileSize(old_sst_file_path);
                        return;
                    }
                }

                if (copyDiskPathViaHardLinking(DiskPath{from.disk, path}, DiskPath{to.disk, to.path / file_name}))
                {
                    ++linked_files_count;
                    linked_files_bytes += from.disk->getFileSize(path);
                    return;
                }
            }
            catch (...)
            {
                /// Fallback to copy if hard link failed, e.g. cross disk hard link is not supported
            }
        }

        /// from_disk:/path/1/rocks/*(file) --(copy)--> to_disk:/path/2/rocks/*(file)
        pool.scheduleOrThrowOnError([&, file_path = path, thread_group = CurrentThread::getGroup()]() {
            if (thread_group)
                CurrentThread::attachToGroupIfDetached(thread_group);
            SCOPE_EXIT_SAFE({
                if (thread_group)
                    CurrentThread::detachFromGroupIfNotDetached();
            });

            setThreadName("CkptCopier");
            from.disk->copyFile(file_path, *to.disk, to.path / file_path.filename());
            copied_files_count.fetch_add(1, std::memory_order_relaxed);
            copied_files_bytes.fetch_add(from.disk->getFileSize(file_path), std::memory_order_relaxed);
        });
    });

    pool.wait();

    auto copied_bytes = copied_files_bytes.load();
    LOG_INFO(
        logger,
        "Copied files count: {}, bytes: {}; Linked files count: {}, bytes: {}; Elapsed {} ms",
        copied_files_count.load(),
        copied_bytes,
        linked_files_count,
        linked_files_bytes,
        stopwatch.elapsedMilliseconds());

    return copied_bytes;
}
}
