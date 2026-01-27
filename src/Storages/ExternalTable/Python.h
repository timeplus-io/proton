#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <Storages/ExternalTable/StorageExternalTable.h>

namespace DB
{
namespace ExternalTable
{
class Python final : public StorageExternalTable
{
public:
    Python(
        const StorageID & table_id,
        const StorageInMemoryMetadata & storage_metadata,
        const ASTCreateQuery & create_query_,
        std::unique_ptr<ExternalTableSettings> settings_,
        bool attach_,
        ContextPtr context_);

    String getType() const override;

    void getTableSchema(ContextPtr, ColumnsDescription & desc) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    std::string python_function;

    void init();

    mutable std::mutex mutex;
    StoragePtr python_storage;
};
}
}

#endif
