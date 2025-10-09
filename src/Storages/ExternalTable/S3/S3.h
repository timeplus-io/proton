#pragma once

#include "config.h"

#if USE_AWS_S3

#include <Storages/ExternalTable/ExternalTableSettings.h>
#include <Storages/ExternalTable/StorageExternalTable.h>

namespace DB
{

namespace ExternalTable
{

class S3 final : public StorageExternalTable
{
public:
    S3(const StorageID &, const StorageInMemoryMetadata &, std::unique_ptr<ExternalTableSettings>, bool attach, ContextPtr);

    ~S3() override = default;

    String getType() const override { return "S3"; }

    bool supportsSubcolumns() const override { return storage_ptr->supportsSubcolumns(); }
    bool supportsStreamingQuery() const override { return false; }

    NamesAndTypesList getVirtuals() const override { return storage_ptr->getVirtuals(); }

    bool supportsPartitionBy() const override { return storage_ptr->supportsPartitionBy(); }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void truncate(
        const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder &) override;

private:
    SinkToStoragePtr writeImpl(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/) override;

    StoragePtr storage_ptr;

    [[maybe_unused]] LoggerPtr logger;
};

}

}

#endif
