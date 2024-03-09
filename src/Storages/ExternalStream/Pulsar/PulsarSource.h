#pragma once

#include <Processors/ISource.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace Poco
{
class Logger;
}

namespace DB
{
struct ExternalStreamSettings;

class Pulsar;

class PulsarSource final : public ISource
{
public:
    PulsarSource(
        Pulsar * pulsar_,
        Block header_,
        ContextPtr query_context_,
        size_t max_block_size
        /*Poco::Logger * log_*/);

    ~PulsarSource() override;

    String getName() const override { return "PulsarSource"; }

    Chunk generate() override;

//    void checkpoint(CheckpointContextPtr ckpt_ctx_) override;
//
//    void recover(CheckpointContextPtr ckpt_ctx_) override;
private:
//    Pulsar * pulsar;
    ContextPtr query_context;
    Chunk head_chunk;
//    size_t max_block_size;
//    Poco::Logger * log;
    DataTypePtr column_type;
};
}
