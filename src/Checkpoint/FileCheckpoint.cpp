#include <Checkpoint/FileCheckpoint.h>

#include <Common/logger_useful.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <base/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_VERSION;
}

FileCheckpoint::FileCheckpoint(VersionType version_, std::function<void(WriteBuffer &)> do_ckpt, bool compressed)
{
    version = version_;
    data.emplace<Entity>(std::move(do_ckpt), compressed);
}

FileCheckpoint::FileCheckpoint(VersionType version_, std::unique_ptr<ReadBuffer> serialized_data, bool compressed)
{
    version = version_;
    data.emplace<SerializedEntity>(std::move(serialized_data), compressed);
}

FileCheckpoint::FileCheckpoint(const DiskPath & ckpt_path)
{
    auto rb = ckpt_path.disk->readFile(ckpt_path.path, ReadSettings{});
    readVarUInt(version, *rb);
    data.emplace<MaterializedEntity>(ckpt_path);
}

void FileCheckpoint::materializeTo(WriteBufferFromFileBase & wb)
{
    /// Invalidate disposable entity after used
    SCOPE_EXIT({ data = std::variant<Entity, SerializedEntity, MaterializedEntity>(); });

    Stopwatch stopwatch;

    auto materialize_func = [&](auto & do_ckpt, bool compressed) {
        /// Format: version + ts + ...(compressed ckpt data)
        writeVarUInt(version, wb);

        Int64 ts = UTCMilliseconds::now();
        writeVarInt(ts, wb);

        if (compressed)
        {
            do_ckpt(wb);

            LOG_DEBUG(
                logger,
                "Took {} ms to materialize file ckpt to {}: size={}",
                stopwatch.elapsedMilliseconds(),
                wb.getFileName(),
                wb.count());
        }
        else
        {
            CompressedWriteBuffer compressed_wb(wb);
            do_ckpt(compressed_wb);
            compressed_wb.finalize();

            LOG_DEBUG(
                logger,
                "Took {} ms to materialize file ckpt to {}: size={} compressed_size={}",
                stopwatch.elapsedMilliseconds(),
                wb.getFileName(),
                compressed_wb.count(),
                wb.count());
        }
    };


    if (auto * entity = std::get_if<Entity>(&data))
    {
        materialize_func(entity->do_ckpt, entity->compressed);
    }
    else if (auto * serialized_entity = std::get_if<SerializedEntity>(&data))
    {
        auto copy_data = [serialized_entity](WriteBuffer & out) { copyData(*serialized_entity->buffer, out); };
        materialize_func(copy_data, serialized_entity->compressed);
    }
    else if (auto * materialized_path = std::get_if<MaterializedEntity>(&data))
    {
        auto file_buf = materialized_path->disk->readFile(materialized_path->path, ReadSettings{});
        copyData(*file_buf, wb);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot materialize an empty checkpoint");
    }
}

size_t FileCheckpoint::materializeTo(const DiskPath & ckpt_path)
{
    if (auto * materialized_path = std::get_if<MaterializedEntity>(&data))
        return copyDiskPath(*materialized_path, ckpt_path, /*try_hard_link_first=*/true);

    auto file_buf = ckpt_path.disk->writeFile(ckpt_path.path);
    materializeTo(*file_buf);
    file_buf->finalize();
    return file_buf->count();
}

void FileCheckpoint::serializeEntity(WriteBuffer & wb, bool compress)
{
    /// Invalidate disposable entity after used
    SCOPE_EXIT({ data = std::variant<Entity, SerializedEntity, MaterializedEntity>(); });

    Stopwatch stopwatch;

    auto serialize_func = [&](auto & do_serialize, bool compressed) {
        auto prev_size = wb.count();
        if (compress == compressed)
        {
            do_serialize(wb);

            LOG_DEBUG(logger, "Took {} ms to serialize file ckpt: size={}", stopwatch.elapsedMilliseconds(), wb.count() - prev_size);
        }
        else if (compress)
        {
            CompressedWriteBuffer compressed_wb(wb);
            do_serialize(compressed_wb);
            compressed_wb.finalize();

            LOG_DEBUG(
                logger,
                "Took {} ms to serialize file ckpt: size={} compressed_size={}",
                stopwatch.elapsedMilliseconds(),
                wb.count() - prev_size,
                compressed_wb.count());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert a compressed buf to an uncompressed buffer");
        }
    };

    if (auto * entity = std::get_if<Entity>(&data))
    {
        serialize_func(entity->do_ckpt, entity->compressed);
    }
    else if (auto * serialized_entity = std::get_if<SerializedEntity>(&data))
    {
        auto copy_data = [serialized_entity](WriteBuffer & out) { copyData(*serialized_entity->buffer, out); };
        serialize_func(copy_data, serialized_entity->compressed);
    }
    else if (auto * materialized_path = std::get_if<MaterializedEntity>(&data))
    {
        auto copy_data = [materialized_path](WriteBuffer & out) {
            auto file_buf = materialized_path->disk->readFile(materialized_path->path, ReadSettings{});
            /// Skip version and ts
            VersionType version_;
            readVarUInt(version_, *file_buf);
            Int64 ts;
            readVarInt(ts, *file_buf);

            copyData(*file_buf, out);
        };
        serialize_func(copy_data, /*compressed=*/true);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize an empty checkpoint");
    }
}

void FileCheckpoint::recover(std::function<void(ReadBuffer &)> do_recover, bool log)
{
    /// Invalidate disposable entity after used
    SCOPE_EXIT({ data = std::variant<Entity, SerializedEntity, MaterializedEntity>(); });

    Stopwatch stopwatch;
    if (auto * serialized_entity = std::get_if<SerializedEntity>(&data))
    {
        auto prev_size = serialized_entity->buffer->count();
        if (serialized_entity->compressed)
        {
            CompressedReadBuffer rb(*serialized_entity->buffer);
            do_recover(rb);

            if (log)
                LOG_INFO(
                    logger,
                    "Took {} ms to recover file ckpt from serialized data: size={} decompressed_size={}",
                    stopwatch.elapsedMilliseconds(),
                    serialized_entity->buffer->count() - prev_size,
                    rb.count());
        }
        else
        {
            do_recover(*serialized_entity->buffer);

            if (log)
                LOG_INFO(
                    logger,
                    "Took {} ms to recover file ckpt from serialized data: size={}",
                    stopwatch.elapsedMilliseconds(),
                    serialized_entity->buffer->count() - prev_size);
        }
    }
    else if (auto * materialized_path = std::get_if<MaterializedEntity>(&data))
    {
        auto file_buf = materialized_path->disk->readFile(materialized_path->path, ReadSettings{});
        /// Skip version and ts
        VersionType recovered_version;
        readVarUInt(recovered_version, *file_buf);

        /// Check version compatibility
        if (recovered_version > version)
            throw Exception(
                ErrorCodes::BAD_VERSION,
                "Failed to recover file checkpoint, incompatible version: {}, expected version: {}",
                recovered_version,
                version);

        Int64 ts;
        readVarInt(ts, *file_buf);

        CompressedReadBuffer rb(*file_buf);
        do_recover(rb);

        if (log)
            LOG_INFO(
                logger,
                "Took {} ms to recover file ckpt from local file '{}': size={}",
                stopwatch.elapsedMilliseconds(),
                file_buf->getFileName(),
                file_buf->count());
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot recover an invalid checkpoint");
    }
}
}
