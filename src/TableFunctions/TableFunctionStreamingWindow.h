#pragma once

#include "ITableFunction.h"

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/StreamingFunctionDescription.h>

namespace DB
{
class TableFunctionStreamingWindow : public ITableFunction
{
public:
    explicit TableFunctionStreamingWindow(const String & name_);
    String getName() const override { return name; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

protected:
    static StorageID resolveStorageID(const ASTPtr & arg, ContextPtr context);

protected:
    StoragePtr executeImpl(const ASTPtr & func_ast, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    void initColumnsDescription(ContextPtr context, ASTPtr streaming_func_ast, const String & func_name_prefix);
    virtual void handleResultType(const ColumnWithTypeAndName & /* type_and_name */) { }

    String name;
    String help_message;

    /// ASTPtr streaming_func_ast;
    /// Names additional_required_columns;
    StreamingFunctionDescriptionPtr streaming_func_desc;

    StorageID storage_id = StorageID::createEmpty();
    ColumnsDescription columns;
};
}
