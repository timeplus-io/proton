#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ExternalStream/Python/StoragePythonTable.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>

namespace DB
{
/// StoragePythonTableWithInput executes a Python external stream function
/// with input data from a source (subquery, table, or stream).
///
/// The input columns are passed as arguments to the Python function,
/// and the function's return value becomes the output rows.
class StoragePythonTableWithInput final : public shared_ptr_helper<StoragePythonTableWithInput>, public IStorage
{
    friend struct shared_ptr_helper<StoragePythonTableWithInput>;

public:
    ~StoragePythonTableWithInput() override = default;

    String getName() const override { return "PythonTableWithInput"; }

    static StoragePtr create(
        const StorageID & table_id,
        const ColumnsDescription & output_columns,
        String function_name_,
        String python_source_,
        ASTPtr source_ast_,
        StoragePtr source_storage_,
        Names input_column_names_,
        PythonTableMode mode_,
        ContextPtr context_);

    bool isRemote() const override { return false; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsStreamingQuery() const override { return true; }
    bool supportsParallelInsert() const override { return false; }
    bool parallelizeOutputAfterReading(ContextPtr) const override { return false; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    StoragePythonTableWithInput(
        const StorageID & table_id,
        const ColumnsDescription & output_columns,
        String function_name_,
        String python_source_,
        ASTPtr source_ast_,
        StoragePtr source_storage_,
        Names input_column_names_,
        PythonTableMode mode_,
        ContextPtr context_);

    /// Name of the Python function to call
    String function_name;

    /// Inline Python source code for the external stream
    String python_source;

    /// Source subquery AST (if source is a subquery)
    ASTPtr source_ast;

    /// Source storage (if source is a table/stream)
    StoragePtr source_storage;

    /// Column names to pass to Python function
    Names input_column_names;

    /// Execution mode for Python result handling
    PythonTableMode mode;
};
}

#endif
