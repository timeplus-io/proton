#pragma once

#include "SourceColumnsDescription.h"

#include <Interpreters/Context_fwd.h>
#include <NativeLog/Record/Record.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{
class StreamingStoreSourceBase : public SourceWithProgress
{
public:
    StreamingStoreSourceBase(const Block & header, const StorageSnapshotPtr & storage_snapshot_, ContextPtr context_);

    Chunk generate() override;

private:
    virtual void readAndProcess() = 0;

protected:
    StorageSnapshot storage_snapshot;

    ContextPtr query_context;

    Chunk header_chunk;

    SourceColumnsDescription columns_desc;

    NameSet required_object_names;

    bool hasObjectColumns() const { return !columns_desc.physical_object_column_names_to_read.empty(); }
    ColumnPtr getSubcolumnFromblock(const Block & block, size_t parent_column_pos, const NameAndTypePair & subcolumn_pair) const;
    void fillAndUpdateObjects(Block & block);

    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;

    /// watermark, only support periodical flush for now
    /// FIXME, late event etc, every second
    Int64 flush_interval_ms = 1000;
    Int64 last_flush_ms = 0;
};
}
