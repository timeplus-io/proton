#pragma once

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
    void calculateColumnPositions(const Block & header, const Block & schema);

protected:
    StorageSnapshot storage_snapshot;

    ContextPtr query_context;

    Chunk header_chunk;

    Names physical_object_column_names_to_read;
    bool hasObjectColumns() const { return !physical_object_column_names_to_read.empty(); }
    ColumnPtr getSubcolumnFromblock(const Block & block, size_t parent_column_pos, const NameAndTypePair & subcolumn_pair) const;
    void fillAndUpdateObjects(Block & block);

    NamesAndTypes subcolumns_to_read;

    /// Column positions requested and returned to downstream
    std::vector<size_t> column_positions;

    /// Column positions to read from file system
    std::vector<uint16_t> physical_column_positions_to_read;

    std::vector<std::function<Int64(const BlockInfo &)>> virtual_time_columns_calc;
    size_t virtual_columns_pos_begin = 0;
    size_t subcolumns_pos_begin = 0;
    /// These virtual columns have the same Int64 type
    DataTypePtr virtual_col_type;

    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;

    /// watermark, only support periodical flush for now
    /// FIXME, late event etc, every second
    Int64 flush_interval_ms = 1000;
    Int64 last_flush_ms = 0;
};
}
