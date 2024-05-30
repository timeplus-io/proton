#pragma once

#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

namespace ExternalStream
{

class Proton final : public StorageExternalStreamImpl
{

public:
    Proton(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, ContextPtr context);
    ~Proton() override = default;

    void startup() override;
    void shutdown() override;

    bool supportsSubcolumns() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context) override;

private:
    bool secure{ false };
    ClusterPtr cluster;
    StorageID remote_stream_id;
    StoragePtr storage_ptr;
    ContextPtr context;
    Poco::Logger * logger;

    ColumnsDescription cached_columns;

};

}

}
