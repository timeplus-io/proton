#pragma once

#include "config.h"

#if USE_MONGODB

#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/StorageMongoDB.h>

namespace DB
{
struct ExternalTableSettings;

namespace ExternalTable
{
class MongoDB final : public StorageExternalTable
{
public:
    MongoDB(
        const StorageID & storage_id,
        const StorageInMemoryMetadata & metadata,
        std::unique_ptr<ExternalTableSettings> settings,
        bool attach_,
        ContextPtr context_);

    std::string getType() const override { return "MongoDB"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        return db->read(column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
    }

    SinkToStoragePtr writeImpl(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context) override
    {
        return db->write(query, metadata_snapshot, local_context);
    }

private:
    std::shared_ptr<StorageMongoDB> db;
};

}
}

#endif
