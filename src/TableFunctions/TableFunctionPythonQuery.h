#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{
class TableFunctionPythonQuery : public ITableFunction
{
public:
    static constexpr auto name = "python";

    std::string getName() const override { return name; }
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "PythonQuery"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    cluster::protocol::UserDefinedFunctionDescriptorPtr udf_desc;
    ColumnsDescription result_columns;
};
}

#endif
