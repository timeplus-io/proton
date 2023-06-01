#pragma once

#include "ITableFunction.h"

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Streaming/FunctionDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{
using TreeRewriterResultPtr = std::shared_ptr<const TreeRewriterResult>;

namespace Streaming
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

    virtual String functionNamePrefix() const = 0;

    FunctionDescriptionPtr createStreamingFunctionDescription(
    ASTPtr ast, TreeRewriterResultPtr syntax_analyzer_result, ContextPtr context, const String & func_name_prefix) const;

private:
    void validateProxyChain() const;

protected:
    String name;

    FunctionDescriptionPtr streaming_func_desc;

    /// Timestamp column expression
    FunctionDescriptionPtr timestamp_func_desc;
    /// Names timestamp_expr_required_columns;
    /// ExpressionActionsPtr timestamp_expr;

    StoragePtr storage;
    ASTPtr subquery;
    StorageID storage_id = StorageID::createEmpty();
    StorageSnapshotPtr underlying_storage_snapshot;
    ColumnsDescription columns;

    /// Nested ProxyStorage for nested table function: tumble(dedup(...))
    StoragePtr nested_proxy_storage;

    bool streaming = true;
};
}
}
