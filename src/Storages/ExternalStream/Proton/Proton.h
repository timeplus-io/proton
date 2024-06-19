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
    Proton(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, bool attach, ContextPtr context);
    ~Proton() override = default;

    String getName() const override { return "TimeplusExternalStream"; }

    void startup() override { storage_ptr->startup(); }
    void shutdown() override { storage_ptr->shutdown(); }
    bool supportsSampling() const override { return storage_ptr->supportsSampling(); }
    bool supportsFinal() const override { return storage_ptr->supportsFinal(); }
    bool supportsPrewhere() const override { return storage_ptr->supportsPrewhere(); }
    bool supportsSubcolumns() const override { return storage_ptr->supportsSubcolumns(); }
    bool supportsDynamicSubcolumns() const override { return true; }

    void read(
        QueryPlan & query_plan,
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
    Poco::Logger * logger;

    ColumnsDescription cached_columns;

};

}

}
