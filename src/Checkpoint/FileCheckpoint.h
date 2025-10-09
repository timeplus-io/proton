#pragma once

#include <Checkpoint/Checkpoint.h>
#include <Common/Logger.h>

namespace DB
{
class WriteBufferFromFile;

class FileCheckpoint;
using FileCheckpointPtr = std::shared_ptr<FileCheckpoint>;

/// A file checkpoint is a simple checkpoint that writes the checkpoint data to a file.
/// FORMAT: <xxx.ckpt>
/// [version][ts][ckpt_data...]
///
/// The ckpt data has three variants depending on its source, which indicates that where the data is located or how it was created:
/// i.   \entity:               create by in memory structures `state data writer`
/// ii.  \serialized_entity:    create by serialized ckpt data
/// iii. \materialized_path:    create from disk `checkpoint file path`
class FileCheckpoint final : public Checkpoint
{
public:
    static constexpr std::string_view CKPT_POSTFIX = ".ckpt";

    explicit FileCheckpoint(const DiskPath & ckpt_path);
    FileCheckpoint(VersionType version_, std::function<void(WriteBuffer &)> do_ckpt, bool compressed = false);
    FileCheckpoint(VersionType version_, std::unique_ptr<ReadBuffer> serialized_data, bool compressed);

    CheckpointType type() const override { return CheckpointType::File; }

    void materializeTo(WriteBufferFromFileBase & file_buf);

    /// \return the actual materialized size in bytes
    size_t materializeTo(const DiskPath & ckpt_path) override;

    void recover(std::function<void(ReadBuffer &)> do_recover, bool log = true);

private:
    void serializeEntity(WriteBuffer & wb, bool compress) override;

    struct Entity
    {
        std::function<void(WriteBuffer &)> do_ckpt; /// state data writer
        bool compressed;
    };

    struct SerializedEntity
    {
        std::unique_ptr<ReadBuffer> buffer; /// serialized ckpt data buffer
        bool compressed;
    };

    using MaterializedEntity = DiskPath; /// checkpoint file path on disk

    std::variant<Entity, SerializedEntity, MaterializedEntity> data;

    LoggerPtr logger = getLogger("FileCheckpoint");
};

}
