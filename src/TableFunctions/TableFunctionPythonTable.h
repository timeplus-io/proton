#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <Interpreters/StorageID.h>
#include <Storages/ExternalStream/Python/StoragePythonTable.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{
/// TableFunctionPythonTable allows calling a Python external stream as a table function
/// with input data from a subquery or stream.
///
/// Usage:
/// CREATE EXTERNAL STREAM py_transform(result Int32)
/// AS $$
/// def py_transform(col1, col2):
///     return [c1 + c2 for c1, c2 in zip(col1, col2)]
/// $$
/// SETTINGS type='python';
///
/// -- Call using python_table (alias python_pipe) function:
/// WITH input AS (SELECT i, j FROM my_stream)
/// SELECT * FROM python_table(py_transform, input, i, j);
///
/// -- Or with direct table reference:
/// SELECT * FROM python_table(py_transform, my_stream, col1, col2);
class TableFunctionPythonTable : public ITableFunction
{
public:
    TableFunctionPythonTable() = default;

    static constexpr auto name = "python_table";

    String getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "PythonTableWithInput"; }

    /// The Python external stream to call
    StorageID python_table_id = StorageID::createEmpty();

    /// The source (subquery or table) to get input data from
    ASTPtr source_ast;
    StoragePtr source_storage;

    /// Column names to pass to Python function
    Names input_column_names;

    /// Output columns description (from the external stream)
    ColumnsDescription output_columns;

    /// Python function name
    String python_function_name;

    /// Inline Python source code
    String python_source;

    /// Execution mode
    PythonTableMode python_mode{PythonTableMode::Auto};
};
}

#endif
