#pragma once

#include "config.h"

#if USE_PULSAR

#    include <Processors/Executors/StreamingFormatExecutor.h>
#    include <Storages/ExternalStream/Pulsar/PulsarSink.h>
#    include <Storages/ExternalStream/Pulsar/PulsarSource.h>
#    include <Storages/ExternalStream/StorageExternalStreamImpl.h>

#    include <pulsar/Client.h>

namespace DB
{

namespace ExternalStream
{

using VirtualColumns = std::map<String, std::pair<DataTypePtr, std::function<Field(const pulsar::Message &)>>>;
using VirtualHeader = std::map<size_t, std::pair<DataTypePtr, std::function<Field(const pulsar::Message &)>>>;

class Pulsar final : public StorageExternalStreamImpl
{
public:
    Pulsar(IStorage * storage, ExternalStreamSettingsPtr settings_, bool attach, ExternalStreamCounterPtr counter, ContextPtr context);
    ~Pulsar() override = default;

    String getName() const override { return "PulsarExternalStream"; }

    void startup() override;
    void shutdown() override;

    NamesAndTypesList getVirtuals() const override;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

private:
    inline const String & getTopic() const { return settings->topic.value; }
    inline const String & serviceUrl() const { return settings->service_url.value; }

    std::vector<String> getPartitions(const ContextPtr & query_context);

    void cacheVirtualColumnNamesAndTypes();
    VirtualHeader calculateVirtualHeader(const Block & header, const Block & non_virtual_header);

    pulsar::ClientConfiguration createClientConfig();

    pulsar::Reader createReader(const ContextPtr & context, const String & partition);
    pulsar::Producer createProducer(const ContextPtr & context);

    NamesAndTypesList virtual_column_names_and_types;
    VirtualColumns virtual_columns;

    std::atomic_flag pulsar_logger_set;
    pulsar::Client client;

    ExternalStreamCounterPtr external_stream_counter;
};

}

}

#endif
