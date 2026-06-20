#pragma once

#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Common/CurrentMetrics.h>

#include <base/shared_ptr_helper.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace DB
{

class StorageExternalTable : public IStorage, public WithContext, public shared_ptr_helper<StorageExternalTable>
{
    friend struct shared_ptr_helper<StorageExternalTable>;

public:
    String getName() const override { return "ExternalTable"; }

    bool isLocal() const override { return false; }
    bool isRemote() const override { return true; }
    bool isExternalTable() const override { return true; }
    bool squashInsert() const noexcept override { return false; }
    void startup() override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/) override;

    void alter(const AlterCommands & params, ContextPtr context_, AlterLockHolder & alter_lock_holder) override;

    virtual String getType() const = 0;

    void updateTableSchema(bool retry_in_background);

protected:
    StorageExternalTable(
        const StorageID & table_id,
        const StorageInMemoryMetadata & storage_metadata,
        std::unique_ptr<ExternalTableSettings> settings_,
        bool attach,
        ContextPtr context_);

    std::unique_ptr<ExternalTableSettings> settings;

private:
    void updateTableSchema();
    /// Get the table schema from the external system.
    virtual void getTableSchema(ContextPtr, ColumnsDescription &) { }

    virtual void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams);

    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method read is not supported by storage {}", getName());
    }

    virtual SinkToStoragePtr writeImpl(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method write is not supported by storage {}", getName());
    }

    const bool attach{false};
    std::unique_ptr<ThreadPool> background_jobs;
};

}
