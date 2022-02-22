#pragma once

#include <DistributedWALClient/Record.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
class StreamingStoreSourceBase : public SourceWithProgress
{
public:
    StreamingStoreSourceBase(const Block & header, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_);

    Chunk generate() override;

private:
    virtual void readAndProcess() = 0;
    void calculateColumnPositions(const Block & header, const Block & schema);

protected:
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;

    Chunk header_chunk;

    /// Column positions requested and returned to downstream
    std::vector<size_t> column_positions;

    /// Column positions to read from file system
    std::vector<uint16_t> physical_column_positions_to_read;

    std::vector<std::function<Int64(const BlockInfo &)>> virtual_time_columns_calc;
    size_t total_physical_columns_in_schema = 0;
    /// These virtual columns have the same Int64 type
    DataTypePtr virtual_col_type;

    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;

    /// watermark, only support periodical flush for now
    // FIXME, late event etc, every second
    Int64 flush_interval_ms = 1000;
    Int64 last_flush_ms = 0;
};
}
