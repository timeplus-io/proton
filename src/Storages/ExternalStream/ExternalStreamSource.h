#pragma once

#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

class ExternalStreamSource
{
public:
    ExternalStreamSource(
        const Block & header_, StorageSnapshotPtr storage_snapshot_, size_t max_block_size_, ContextPtr query_context_);
    virtual ~ExternalStreamSource() = default;

protected:
    /// Returns a pair of format executors: {regular_executor, batch_executor}.
    /// The batch_executor is nullptr if batch parsing is not applicable for the given format.
    /// Requires getPhysicalHeader() to be called first.
    std::pair<std::shared_ptr<StreamingFormatExecutor>, std::shared_ptr<StreamingFormatExecutor>>
    getInputFormatExecutor(const String & data_format, const FormatSettings & format_settings);

    Block header;
    StorageSnapshotPtr storage_snapshot;

    Chunk header_chunk;
    Block physical_header;

    size_t max_block_size;

    bool parse_in_batch;
    bool request_virtual_columns = false;

    ContextPtr query_context;

    /// 'empty_read_buffer' is a dependency of the created format executor from getInputFormatExecutor()
    /// It is not actually used to read data, because the format executor will be fed in the derived class
    ReadBufferFromMemory empty_read_buffer{"", 0};

    /// Initialize physical_header. Must be called before getInputFormatExecutor().
    /// Derived classes can override to customize behavior.
    virtual void getPhysicalHeader();
};

}
