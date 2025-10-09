#pragma once

#include <Checkpoint/CheckpointType.h>
#include <Checkpoint/DiskPath.h>

#include <base/types.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;

class Checkpoint;
using CheckpointPtr = std::shared_ptr<Checkpoint>;
using CheckpointConstPtr = std::shared_ptr<const Checkpoint>;

/// Base class for all checkpoints.
/// An example:
/// {
///     /// Create a file checkpoint
///     String state = "hello";
///     CheckpointPtr ckpt = std::make_shared<FileCheckpoint>(/*version=*/1, [&](WriteBuffer & wb) { writeStringBinary(state, wb); });
///
///     /// a. For disk-based checkpoint storages (E.g. LocalCheckpointStorage/S3CheckpointStorage), use materializeTo()/createFrom() to persist/load the checkpoint
///     {
///         /// Materialize the checkpoint to local disk
///         ckpt->materializeTo("/path/key.ckpt");
///
///         /// Load the checkpoint from local disk
///         ckpt = Checkpoint::createFrom(CheckpointType::File, "/path/key.ckpt");
///     }
///
///     /// b. For other checkpoint storages, use serialize()/deserialize() to persist/load the checkpoint
///     {
///         /// Serialize the checkpoint
///         String serialized_data;
///         WriteBufferFromString wb(serialized_data);
///         ckpt->serialize(wb, /*compress_entity=*/true);
///
///         /// Deserialize from serialized data
///         ckpt = Checkpoint::deserialize(std::make_unique<ReadBufferFromString>(serialized_data), /*entity_compressed=*/true);
///     }
///
///     /// Recover from the checkpoint
///     String recovered_state;
///     std::static_pointer_cast<FileCheckpoint>(ckpt)->recover([&](ReadBuffer & rb) { readStringBinary(recovered_state, rb); });
///     assert(recovered_state == state);
/// }
class Checkpoint
{
public:
    virtual ~Checkpoint() = default;
    VersionType getVersion() const { return version; }

    static const std::unordered_map<CheckpointType, std::string_view> & supportedTypesAndPostfixes();
    static std::string_view postfix(CheckpointType type);

    virtual CheckpointType type() const = 0;

    /// Incremental checkpoint support
    virtual bool supportsIncremental() const noexcept { return false; }
    virtual bool enableIncremental(CheckpointConstPtr /*prev_ckpt_*/) { return false; }
    virtual bool isIncremental() const { return false; }

    void serialize(WriteBuffer & wb, bool compress_entity);
    static CheckpointPtr deserialize(std::unique_ptr<ReadBuffer> ckpt_buf, bool entity_compressed);

    /// Materialize the checkpoint to \ckpt_path on disk
    /// \return the actual materialized size in bytes
    virtual size_t materializeTo(const DiskPath & ckpt_path) = 0;
    static CheckpointPtr createFrom(CheckpointType type, const DiskPath & ckpt_path);

protected:
    virtual void serializeEntity(WriteBuffer & wb, bool compress) = 0;

    VersionType version = 0;
};
}
