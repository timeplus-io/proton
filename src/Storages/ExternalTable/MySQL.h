#pragma once

#include "config.h"
#if USE_MYSQL

#    include <Storages/ExternalTable/StorageExternalTable.h>
#    include <Storages/StorageMySQL.h>

#    include <memory>
#    include <mutex>


namespace DB
{
struct ExternalTableSettings;

namespace ExternalTable
{
/// MySQL is a simple external table adapter which forwards the calls to wrapped StorageMySQL.
class MySQL final : public StorageExternalTable
{
public:
    static constexpr uint64_t DEFAULT_CONNECTION_POOL_SIZE = 16;

    MySQL(
        const StorageID & table_id,
        const StorageInMemoryMetadata & storage_metadata,
        std::unique_ptr<ExternalTableSettings> settings_,
        bool attach_,
        const ContextPtr & context_);

    std::string getType() const override { return "MySQL"; }

    void getTableSchema(ContextPtr /*local_context*/, ColumnsDescription & desc) override
    {
        init();
        desc = mysql->getInMemoryMetadataPtr()->getColumns();
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
        return mysql->read(
            column_names, storage_snapshot, query_info, std::move(local_context), processed_stage, max_block_size, num_streams);
    }

    SinkToStoragePtr writeImpl(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context) override
    {
        init();
        return mysql->write(query, metadata_snapshot, std::move(local_context));
    }

private:
    void init();

    mutable std::mutex mutex;
    std::shared_ptr<StorageMySQL> mysql;
};
}
}
#endif
