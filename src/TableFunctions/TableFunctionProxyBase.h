#pragma once

#include "ITableFunction.h"

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Streaming/StreamingFunctionDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
class TableFunctionProxyBase : public ITableFunction
{
public:
    explicit TableFunctionProxyBase(const String & name_);
    String getName() const override { return name; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    virtual StoragePtr calculateColumnDescriptions(ContextPtr context);

protected:
    void resolveStorageID(const ASTPtr & arg, ContextPtr context);

    StoragePtr
    executeImpl(const ASTPtr & func_ast, ContextPtr context, const String & table_name, ColumnsDescription cached_columns) const override;

private:
    void validateProxyChain() const;

protected:
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

    /// Nested ProxyStorage for nested table function: tumble(dedup(...))
    StoragePtr nested_proxy_storage;

    bool streaming = true;
};
}
