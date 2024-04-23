#pragma once

#include <Processors/ISource.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>

#include <pulsar/Client.h>

namespace Poco
{
class Logger;
}

namespace DB
{
struct ExternalStreamSettings;

class Pulsar;
class StreamingFormatExecutor;

class PulsarSource final : public ISource
{
public:
    PulsarSource(
        Pulsar * pulsar_,
        Block header_,
        const StorageSnapshotPtr & storage_snapshot,
        ContextPtr query_context_,
        Poco::Logger * log_,
        ExternalStreamCounterPtr external_stream_counter_,
        size_t max_block_size
       );

    ~PulsarSource() override;

    String getName() const override { return "PulsarSource"; }

    Chunk generate() override;

//    void checkpoint(CheckpointContextPtr ckpt_ctx_) override;
//
//    void recover(CheckpointContextPtr ckpt_ctx_) override;
private:
    void initConsumer(/* const Pulsar * pulsar */);
    void initFormatExecutor(const Pulsar * pulsar_);
    inline void readAndProcess();

    Pulsar * pulsar;
    ContextPtr query_context;
    Chunk head_chunk;
    Poco::Logger * log;
    DataTypePtr column_type;

    Block header;
    std::shared_ptr<ExpressionActions> convert_non_virtual_to_physical_action = nullptr;
    bool request_virtual_columns = false;
    std::vector<std::function<Field(const pulsar::Message)>> virtual_col_value_functions;
    std::vector<DataTypePtr> virtual_col_types;

    std::vector<Chunk> result_chunks;
    std::vector<Chunk>::iterator iter;
    MutableColumns current_batch;

    Chunk header_chunk;

    pulsar::Consumer consumer;
    std::unique_ptr<StreamingFormatExecutor> format_executor;

    std::optional<String> format_error;

    ExternalStreamCounterPtr external_stream_counter;
    size_t max_block_size;
    ReadBufferFromMemory read_buffer;
    Block physical_header;
    StorageSnapshotPtr storage_snapshot;
    const Block non_virtual_header;
};
}

