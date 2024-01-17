#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>
#include <Storages/ExternalTable/ExternalTableImpl.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>

namespace DB
{

class StorageExternalTable final : public shared_ptr_helper<StorageExternalTable>, public IStorage, public WithContext
{
    friend struct shared_ptr_helper<StorageExternalTable>;

public:
    String getName() const override { return "ExternalTable"; }

    bool isRemote() const override { return true; }
    bool isExternalTable() const override { return true; }

    void startup() override { external_table->startup(); }
    void shutdown() override { external_table->shutdown(); }

    SinkToStoragePtr write(
        const ASTPtr & /*query*/,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr /*context*/) override;

protected:
    StorageExternalTable(
        const StorageID & table_id_,
        std::unique_ptr<ExternalTableSettings> settings,
        ContextPtr context_);

private:
    IExternalTablePtr external_table;

    /// TBD
    // Pipe read(
    //     const Names & /*column_names*/,
    //     const StorageSnapshotPtr & /*storage_snapshot*/,
    //     SelectQueryInfo & /*query_info*/,
    //     ContextPtr /*context*/,
    //     QueryProcessingStage::Enum /*processed_stage*/,
    //     size_t /*max_block_size*/,
    //     size_t /*num_streams*/) override;
};

}
