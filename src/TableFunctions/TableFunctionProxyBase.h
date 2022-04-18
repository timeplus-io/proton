#pragma once

#include "ITableFunction.h"

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Streaming/StreamingFunctionDescription.h>
#include <Interpreters/Streaming/StreamingWindowCommon.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
class DataTypeTuple;

class TableFunctionProxyBase : public ITableFunction
{
public:
    explicit TableFunctionProxyBase(const String & name_);
    String getName() const override { return name; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

protected:
    StorageID resolveStorageID(const ASTPtr & arg, ContextPtr context);

    void doParseArguments(const ASTPtr & func_ast, ContextPtr context, const String & help_msg);

    virtual void postArgs(ASTs &) const { }

    virtual String functionNamePrefix() const = 0;

    virtual ASTs checkAndExtractArguments(ASTFunction *) const { return {}; }

protected:
    StoragePtr
    executeImpl(const ASTPtr & func_ast, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;
    virtual void init(ContextPtr context, ASTPtr streaming_func_ast, const String & func_name_prefix, ASTPtr timestamp_expr_ast);
    virtual void handleResultType(const ColumnWithTypeAndName & type_and_name);
    virtual DataTypePtr getElementType(const DataTypeTuple * tuple) const = 0;

    String name;

    StreamingFunctionDescriptionPtr streaming_func_desc;

    /// Timestamp column expression
    StreamingFunctionDescriptionPtr timestamp_func_desc;
    /// Names timestamp_expr_required_columns;
    /// ExpressionActionsPtr timestamp_expr;

    ASTPtr subquery;
    StorageID storage_id = StorageID::createEmpty();
    StorageMetadataPtr underlying_storage_metadata_snapshot;
    ColumnsDescription columns;

    bool streaming = true;
};
}
