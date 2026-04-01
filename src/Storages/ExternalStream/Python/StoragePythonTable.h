#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/PythonModuleSession.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>

namespace DB
{
/// Execution mode for Python external stream or table function
enum class PythonTableMode
{
    Auto, /// Auto-detect: generator = streaming, list = batch
    Streaming, /// Force streaming mode (expect generator)
    Batch /// Force batch mode (expect list)
};

class StoragePythonTable final : public shared_ptr_helper<StoragePythonTable>, public IStorage
{
    friend struct shared_ptr_helper<StoragePythonTable>;

public:
    ~StoragePythonTable() override = default;

    String getName() const override { return "PythonTable"; }

    static StoragePtr create(
        const StorageID & table_id,
        const ColumnsDescription & columns,
        cpython::PythonFunction function_,
        PythonTableMode mode_ = PythonTableMode::Auto,
        String sink_function_name_ = {});

    bool isRemote() const override { return false; }
    bool isLocal() const override { return false; } /// Needs to be replicated across cluster nodes
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

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    const cpython::PythonFunction & getFunction() const { return function; }

    PythonTableMode getMode() const { return mode; }

private:
    StoragePythonTable(
        const StorageID & table_id,
        const ColumnsDescription & columns,
        cpython::PythonFunction function_,
        PythonTableMode mode_,
        String sink_function_name_);

    /// Convert Python result to Block (for batch mode)
    Block convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const;

    const cpython::PythonFunction function;
    PythonTableMode mode;
    const String sink_function_name;
};
}

#endif
