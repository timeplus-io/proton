#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>

namespace DB
{
class StoragePythonTable final : public shared_ptr_helper<StoragePythonTable>, public IStorage
{
    friend struct shared_ptr_helper<StoragePythonTable>;

public:
    ~StoragePythonTable() override = default;

    String getName() const override { return "PythonTable"; }

    static StoragePtr create(const StorageID & table_id, const ColumnsDescription & columns, String function_name_, String source_code_);

    bool isRemote() const override { return false; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsStreamingQuery() const override { return true; }
    bool supportsParallelInsert() const override { return false; }
    bool parallelizeOutputAfterReading(ContextPtr) const override { return false; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    StoragePythonTable(const StorageID & table_id, const ColumnsDescription & columns, String function_name_, String source_code_);

    Block executePython(ContextPtr context) const;

    const String function_name;
    const String source_code;
};
}

#endif
