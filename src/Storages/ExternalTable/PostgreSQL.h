#pragma once

#include "config.h"

#if USE_LIBPQXX
#    include <Storages/ExternalTable/StorageExternalTable.h>
#    include <Storages/StoragePostgreSQL.h>


namespace DB
{
struct ExternalTableSettings;
using ExternalTableSettingsPtr = std::unique_ptr<ExternalTableSettings>;

namespace ExternalTable
{
/// PostgreSQL is a simple external table adapter which forwards the calls to wrapped StoragePostgreSQL.
class PostgreSQL final : public StorageExternalTable
{
public:
    static constexpr uint64_t DEFAULT_CONNECTION_POOL_SIZE = 16;

    PostgreSQL(
        const StorageID & table_id,
        const StorageInMemoryMetadata & storage_metadata,
        ExternalTableSettingsPtr settings_,
        bool attach_,
        const ContextPtr & context_)
        : StorageExternalTable(table_id, storage_metadata, std::move(settings_), attach_, context_)
    {
    }

    std::string getType() const override { return "PostgreSQL"; }

    void getTableSchema(ContextPtr /*local_context*/, ColumnsDescription & desc) override
    {
        init();
        desc = storage->getInMemoryMetadataPtr()->getColumns();
    }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        init();
        return storage->read(
            column_names, storage_snapshot, query_info, std::move(local_context), processed_stage, max_block_size, num_streams);
    }

    SinkToStoragePtr writeImpl(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context) override
    {
        init();
        return storage->write(query, metadata_snapshot, std::move(local_context));
    }

private:
    void init();

    mutable std::mutex mutex;
    std::unique_ptr<StoragePostgreSQL> storage;
};
}
}
#endif
