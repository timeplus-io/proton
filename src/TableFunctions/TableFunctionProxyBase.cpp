#include "TableFunctionProxyBase.h"

#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/StorageView.h>
#include <Storages/Streaming/ProxyStream.h>
#include <Storages/Streaming/storageUtil.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{

void TableFunctionProxyBase::resolveStorageID(const ASTPtr & arg, ContextPtr context)
{
    if (auto * sub = arg->as<ASTSubquery>())
    {
        subquery = sub->clone();
        storage_id = StorageID::createEmpty();
        /// TODO: Whether the temporary table should be create in temporary database?
        storage_id.database_name = context->getCurrentDatabase();
        storage_id.table_name = sub->cte_name;
    }
    else if (arg->as<ASTFunction>())
    {
        /// tumble(table(devices), ...)
        auto query_context = context->getQueryContext();
        const auto & function_storage = query_context->executeTableFunction(arg);
        if (auto * stream_storage = function_storage->as<ProxyStream>())
        {
            streaming = stream_storage->isStreaming();
            nested_proxy_storage = function_storage;
        }
        storage_id = function_storage->getStorageID();
    }
    else if (auto * identifier = arg->as<ASTIdentifier>())
    {
        storage_id = identifier->createTable()->getTableId();
        if (storage_id.database_name.empty())
            storage_id.database_name = context->getCurrentDatabase();

        /// return the storage ID with UUID
        storage_id.uuid = DatabaseCatalog::instance().getTable(storage_id, context)->getStorageID().uuid;
    }
    else
    {
        throw Exception("First argument must be stream name", ErrorCodes::BAD_ARGUMENTS);
    }
}

StoragePtr TableFunctionProxyBase::calculateColumnDescriptions(ContextPtr context)
{
    StoragePtr storage;

    if (subquery)
    {
        SelectQueryOptions options;
        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(subquery->children[0], context, options.subquery());
        if (interpreter_subquery)
        {
            auto source_header = interpreter_subquery->getSampleBlock();
            columns = ColumnsDescription(source_header.getNamesAndTypesList());

            /// determine whether it is a streaming query
            streaming = interpreter_subquery->isStreaming();
        }
    }
    else
    {
        storage = DatabaseCatalog::instance().getTable(storage_id, context);
        if (!supportStreamingQuery(storage))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Doesn't support apply {} to storage '{}'", getName(), storage->getName());

        if (storage->as<StorageView>())
        {
            underlying_storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr());
            auto select = underlying_storage_snapshot->getMetadataForQuery()->getSelectQuery().inner_query;
            SelectQueryOptions options;
            auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(select, context, options);
            if (interpreter_subquery)
            {
                auto source_header = interpreter_subquery->getSampleBlock();
                columns = ColumnsDescription(source_header.getNamesAndTypesList());

                /// determine whether it is a streaming query
                streaming = interpreter_subquery->isStreaming();
            }
        }
        else
        {
            underlying_storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr());
            columns = underlying_storage_snapshot->metadata->getColumns();
        }
    }

    return storage;
}

TableFunctionProxyBase::TableFunctionProxyBase(const String & name_) : name(name_)
{
}

StoragePtr TableFunctionProxyBase::executeImpl(
    const ASTPtr & /* func_ast */, ContextPtr context, const String & /* table_name */, ColumnsDescription /* cached_columns_ = {} */) const
{
    return ProxyStream::create(
        storage_id,
        columns,
        underlying_storage_snapshot,
        context,
        streaming_func_desc,
        timestamp_func_desc,
        nested_proxy_storage,
        getName(),
        subquery,
        streaming);
}

ColumnsDescription TableFunctionProxyBase::getActualTableStructure(ContextPtr /* context */) const
{
    return columns;
}
}
}
