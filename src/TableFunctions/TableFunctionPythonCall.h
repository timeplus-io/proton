#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Core/Types.h>
#include <TableFunctions/ITableFunction.h>


namespace DB
{

/* call('function_name', structure, batch, limit) - creates a temporary call storage
 *
 * Used for alerts.
 */
class TableFunctionPythonCall : public ITableFunction
{
public:
    static constexpr auto name = "call";
    std::string getName() const override { return name; }
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "PythonCall"; }

    ColumnsDescription getActualTableStructure(ContextPtr) const override;

    cluster::protocol::UserDefinedFunctionDescriptorPtr udf_desc;
};

}

#endif
