#pragma once

#include <Checkpoint/CheckpointRequest.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <Cluster/SchemaRecord/SourceColumnsDescription.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Streaming/ISource.h>
#include <Storages/Stream/InMemoryIdempotentKeys.h>
#include <Storages/Stream/StreamingBlockReaderBase.h>

namespace DB
{

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class StreamingStoreSourceBase : public Streaming::ISource
{
public:
    StreamingStoreSourceBase(
        const Block & header, const StorageSnapshotPtr & storage_snapshot_, ContextPtr context_, LoggerPtr logger_, ProcessorID pid_);

    Chunk generate() override;

    void setIdempotentKeys(InMemoryIdempotentKeysPtr idem_keys) { idempotent_keys.swap(idem_keys); }

protected:
    void process(cluster::SchemaRecordPtrs & records);

private:
    virtual void readAndProcess() = 0;
    virtual std::pair<String, Int32> getStreamShard() const = 0;
    virtual Int64 lastFetchedSN() const = 0;

    void doCheckpoint(CheckpointContextPtr ckpt_ctx_) override;
    void doRecover(CheckpointContextPtr ckpt_ctx_) override;

    bool dedupBlock(const cluster::SchemaRecordPtr & record);

protected:
    StorageSnapshotPtr storage_snapshot;

    ContextPtr query_context;

    Chunk header_chunk;

    cluster::SourceColumnsDescription columns_desc;

    /// Idempotent keys caching
    InMemoryIdempotentKeysPtr idempotent_keys;

    ColumnPtr getSubcolumnFromBlock(const Block & block, size_t parent_column_pos, const NameAndTypePair & subcolumn_pair) const;

    std::vector<std::pair<Chunk, Int64>> result_chunks_with_sns;
    std::vector<std::pair<Chunk, Int64>>::iterator iter;
};
}
