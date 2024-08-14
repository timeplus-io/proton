#pragma once

#include <Storages/Streaming/SourceColumnsDescription.h>

#include <Checkpoint/CheckpointRequest.h>
#include <Interpreters/Context_fwd.h>
#include <NativeLog/Record/Record.h>
#include <Processors/Streaming/ISource.h>

namespace DB
{

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class StreamingStoreSourceBase : public Streaming::ISource
{
public:
    StreamingStoreSourceBase(
        const Block & header, const StorageSnapshotPtr & storage_snapshot_, ContextPtr context_, Poco::Logger * log_, ProcessorID pid_);

    Chunk generate() override;

private:
    virtual void readAndProcess() = 0;
    virtual std::pair<String, Int32> getStreamShard() const = 0;

    Chunk doCheckpoint(CheckpointContextPtr ckpt_ctx_) override;
    void doRecover(CheckpointContextPtr ckpt_ctx_) override;

protected:
    StorageSnapshotPtr storage_snapshot;

    ContextPtr query_context;

    Poco::Logger * logger;

    Chunk header_chunk;

    SourceColumnsDescription columns_desc;

    bool hasDynamicSubcolumns() const { return !columns_desc.physical_object_columns_to_read.empty(); }
    ColumnPtr getSubcolumnFromBlock(const Block & block, size_t parent_column_pos, const NameAndTypePair & subcolumn_pair) const;
    void fillAndUpdateObjectsIfNecessary(Block & block);

    std::vector<std::pair<Chunk, Int64>> result_chunks_with_sns;
    std::vector<std::pair<Chunk, Int64>>::iterator iter;
};
}
