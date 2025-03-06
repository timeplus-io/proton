#pragma once

#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/StorageSnapshot.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{

class ExternalStreamSource
{
public:
    ExternalStreamSource(
        const Block & header_, const StorageSnapshotPtr & storage_snapshot_, size_t max_block_size_, ContextPtr query_context_);
    virtual ~ExternalStreamSource() = default;

protected:
    void initInputFormatExecutor(const String & data_format, const FormatSettings & format_settings);

    Block header;
    StorageSnapshotPtr storage_snapshot;

    Chunk header_chunk;
    Block physical_header;

    size_t max_block_size;

    /// read_buffer is a dependency of format_executor, make sure we keep them in order.
    ReadBufferFromMemory read_buffer;
    std::unique_ptr<StreamingFormatExecutor> format_executor;
    std::optional<String> format_error;

    ContextPtr query_context;

private:
    virtual void getPhysicalHeader();
};

}
