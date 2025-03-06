#pragma once

#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/StorageProxy.h>

namespace DB
{

namespace ExternalStream
{

class Timeplus final : public StorageProxy
{
public:
    Timeplus(
        IStorage * storage,
        StorageInMemoryMetadata & storage_metadata,
        std::unique_ptr<ExternalStreamSettings> settings_,
        bool attach,
        ContextPtr context);
    ~Timeplus() override = default;

    String getName() const override { return "TimeplusExternalStream"; }
    StoragePtr getNested() const override { return storage_ptr; }

    bool squashInsert() const noexcept override { return false; }
    bool supportsSubcolumns() const override { return storage_ptr->supportsSubcolumns(); }
    bool supportsStreamingQuery() const override { return true; }

    void drop() override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_) override;

    bool isSecure() const { return secure; }

private:
    StorageID remote_stream_id;
    StoragePtr storage_ptr;

    const fs::path cache_dir;

    bool secure;

    Poco::Logger * logger;
};

}

}
