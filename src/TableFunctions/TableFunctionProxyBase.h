#pragma once

#include "ITableFunction.h"

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Interpreters/Streaming/TimestampFunctionDescription.h>
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

    TableFunctionDescriptionPtr createStreamingTableFunctionDescription(ASTPtr ast, ContextPtr context) const;

private:
    void validateProxyChain() const;

protected:
    String name;

    TableFunctionDescriptionPtr streaming_func_desc;

    /// Timestamp column expression
    TimestampFunctionDescriptionPtr timestamp_func_desc;

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
