#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/Streaming/TableFunctionDescription_fwd.h>
#include <Interpreters/Streaming/TimestampFunctionDescription_fwd.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>
#include <Core/Streaming/DataStreamSemantic.h>

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

    TableFunctionDescriptionMutablePtr createStreamingTableFunctionDescription(ASTPtr ast, ContextPtr context) const;

private:
    void validateProxyChain() const;

protected:
    String name;

    TableFunctionDescriptionMutablePtr streaming_func_desc;

    /// Timestamp column expression
    TimestampFunctionDescriptionMutablePtr timestamp_func_desc;

    StoragePtr storage;
    ASTPtr subquery;
    StorageID storage_id = StorageID::createEmpty();
    StorageSnapshotPtr underlying_storage_snapshot;
    ColumnsDescription columns;

    /// Nested ProxyStorage for nested table function: tumble(dedup(...))
    StoragePtr nested_proxy_storage;

    bool streaming = true;
    DataStreamSemantic data_stream_semantic = DataStreamSemantic::Append;
};
}
}
