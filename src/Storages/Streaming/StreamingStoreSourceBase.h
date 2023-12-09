#pragma once

#include <Storages/Streaming/SourceColumnsDescription.h>

#include <Checkpoint/CheckpointRequest.h>
#include <Interpreters/Context_fwd.h>
#include <NativeLog/Record/Record.h>
#include <Processors/ISource.h>

namespace DB
{

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class StreamingStoreSourceBase : public ISource
{
public:
    StreamingStoreSourceBase(
        const Block & header,
        const StorageSnapshotPtr & storage_snapshot_,
        bool enable_partial_read,
        ContextPtr context_,
        Poco::Logger * log_,
        ProcessorID pid_);

    Chunk generate() override;

    void checkpoint(CheckpointContextPtr ckpt_ctx_) override;

    void recover(CheckpointContextPtr ckpt_ctx_) override;

private:
    virtual void readAndProcess() = 0;
    virtual std::pair<String, Int32> getStreamShard() const = 0;

    Chunk doCheckpoint(CheckpointContextPtr ckpt_ctx_);

protected:
    StorageSnapshotPtr storage_snapshot;

    ContextPtr query_context;

    Poco::Logger * log;

    Chunk header_chunk;

    SourceColumnsDescription columns_desc;

    bool hasDynamicSubcolumns() const { return !columns_desc.physical_object_columns_to_read.empty(); }
    ColumnPtr getSubcolumnFromBlock(const Block & block, size_t parent_column_pos, const NameAndTypePair & subcolumn_pair) const;
    void fillAndUpdateObjectsIfNecessary(Block & block);

    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;

    Int64 last_sn = -1;
    Int64 last_epoch = -1;
    /// For checkpoint
    CheckpointRequest ckpt_request;
};
}
