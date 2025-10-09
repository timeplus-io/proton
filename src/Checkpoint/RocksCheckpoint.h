#pragma once

#include <Checkpoint/Checkpoint.h>
#include <Common/Logger.h>
#include <Common/Rocks/RocksHandler.h>

namespace rocksdb
{
class DB;
}

namespace DB
{
class RocksCheckpoint;
using RocksCheckpointPtr = std::shared_ptr<RocksCheckpoint>;

/// A rocks checkpoint of the rocksdb instance.
/// FORMAT: <xxx.rckpt>/
///  |-- VERSION
///  |-- CURRENT
///  |-- MANIFEST-XXXXXX
///  |-- OPTIONS-XXXXXX
///  |-- xxx.sst
///
/// The ckpt data has three variants depending on its source, which indicates that where the data is located or how it was created:
/// i.   \entity:               create by in memory structures `Rocks`
/// ii.  \serialized_entity:    create by serialized ckpt data
/// iii. \materialized_path:    create from disk `rocks checkpoint directory path`
///
/// **Supports incremental**
/// An example:
/// {
///     /// rocks: a rocksdb instance with key-value pairs: k1->v1, k2->v2
///
///     /// Create a rocks checkpoint to disk ``/path/1/key.rckpt`
///     CheckpointPtr ckpt_1 = std::make_shared<RocksCheckpoint>(1, rocks);
///     ckpt_1->materializeTo("/path/1/key.rckpt");
///
///     /// rocks: put k3->v3 and create a new checkpoint
///     CheckpointPtr ckpt_2 = std::make_shared<RocksCheckpoint>(1, rocks);
///
///     /// Serialize rocks checkpoint to buffer
///     String serialized_delta_data;
///     {
///         /// Enable incremantal with previous ckpt_1
///         ckpt_1 = Checkpoint::createFrom(CheckpointType::Rocks, "/path/1/key.rckpt");
///         auto enabled = ckpt_2->enableIncremental(ckpt_1);
///         assert(enabled);
///         assert(ckpt_2->isIncremental());
///
///         /// Serialize rocks files exclude sst files existed in previous checkpoint, for example:
///         /// (Previous rocks ckpt_1)           (Current rocks ckpt_2)
///         ///  .../001.sst                    .../001.sst  (to skip)
///         ///  .../002.sst                    .../002.sst  (to skip)
///         ///                                 .../003.sst
///         ///  .../VERSION                    .../VERSION
///         ///  .../CURRENT                    .../CURRENT
///         ///  .../MANIFEST-000005            .../MANIFEST-000005
///         ///  .../OPTIONS-000009             .../OPTIONS-000009
///         /// Now we only serialize files exclude 001.sst, 002.sst
///         WriteBufferWithString wb(serialized_delta_data);
///         ckpt_2->serialize(wb, /*compress_entity=*/true);
///     }
///
///     /// Deserialize rocks files and materialize rocks checkpoint to disk `/path/2/key.rckpt`
///     {
///         ckpt_2 = Checkpoint::deserialize(std::make_unique<ReadBufferFromString>(serialized_delta_data), /*entity_compressed=*/true);
///         /// it's a delta ckpt, we need set previous ckpt_1 as a reference
///         ckpt_1 = Checkpoint::createFrom(CheckpointType::Rocks, "/path/1/key.rckpt");
///         auto enabled = ckpt_2->enableIncremental(ckpt_1);
///         assert(enabled);
///         assert(ckpt_2->isIncremental());
///
///         /// 1) Link sst files of ckpt_1 from "/path/1/key.rckpt/" to "/path/2/key.rckpt/"
///         ///    (i.e. 001.sst, 002.sst)
///         /// 2) Deserialize incremental rocks files and materialize to "/path/2/key.rckpt/"
///         ///    (i.e. 003.sst, VERSION, CURRENT, MANIFEST-000005, OPTIONS-000009)
///         ckpt_2->materializeTo("/path/2/key.rckpt");
///     }
///
///     /// Recover rocks checkpoint from disk with key-value pairs: k1->v1, k2->v2, k3->v3
///     ckpt_2 = Checkpoint::createFrom(CheckpointType::Rocks, "/path/2/key.rckpt");
///     static_pointer_cast<RocksCheckpoint>(ckpt_2)->recover("/path/recovred_rocks_dir");
/// }
class RocksCheckpoint : public Checkpoint
{
public:
    static constexpr std::string_view CKPT_POSTFIX = ".rckpt";

    explicit RocksCheckpoint(const DiskPath & ckpt_path);
    RocksCheckpoint(VersionType version_, RocksPtr rocks);
    RocksCheckpoint(VersionType version_, std::unique_ptr<ReadBuffer> serialized_data, bool compressed);

    CheckpointType type() const override { return CheckpointType::Rocks; }

    bool supportsIncremental() const noexcept override { return true; }
    bool enableIncremental(CheckpointConstPtr prev_ckpt_) override;
    bool isIncremental() const override { return prev_ckpt_path.has_value(); }

    /// \return the actual materialized size in bytes
    size_t materializeTo(const DiskPath & ckpt_path) override;

    void recover(const std::filesystem::path & recovered_path);

private:
    struct Entity
    {
        RocksPtr rocks;
    };

    struct SerializedEntity
    {
        std::unique_ptr<ReadBuffer> buffer; /// serialized ckpt data buffer
        bool compressed;
    };

    using MaterializedEntity = DiskPath; /// checkpoint file path on disk

    void serializeEntity(WriteBuffer & wb, bool compress) override;

    /// \return the copied bytes (exclude linked files)
    size_t copyRocks(const DiskPath & from, const DiskPath & to) const;
    /// \return the deserialized and copied bytes (exclude linked files)
    size_t deserializeEntity(SerializedEntity & serialized_entity, const DiskPath & ckpt_path);

    std::variant<Entity, SerializedEntity, MaterializedEntity> data;

    std::optional<DiskPath> prev_ckpt_path;
    std::unordered_set<std::string> old_sst_files; /// sst files of previous checkpoint

    LoggerPtr logger = getLogger("RocksCheckpoint");
};
}
