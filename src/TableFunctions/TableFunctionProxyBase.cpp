#include <TableFunctions/TableFunctionProxyBase.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Interpreters/TreeRewriter.h>
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
            streaming = stream_storage->isStreamingQuery();
            nested_proxy_storage = function_storage;

            auto proxy = stream_storage->getProxyStorageOrSubquery();
            if (const auto * nested_storage = std::get_if<StoragePtr>(&proxy))
                storage = *nested_storage;
            else if (const auto * nested_subquery = std::get_if<ASTPtr>(&proxy))
                subquery = *nested_subquery;
        }
        storage_id = function_storage->getStorageID();
    }
    else if (auto storage_id_opt = tryGetStorageID(arg))
    {
        storage_id = *storage_id_opt;
        if (storage_id.database_name.empty())
            storage_id.database_name = context->getCurrentDatabase();

        /// return the storage ID with UUID
        storage = DatabaseCatalog::instance().getTable(storage_id, context);
        storage_id.uuid = storage->getStorageID().uuid;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument '{}' must be stream name", arg->dumpTree());
    }
}

StoragePtr TableFunctionProxyBase::calculateColumnDescriptions(ContextPtr context)
{
    if (nested_proxy_storage)
    {
        underlying_storage_snapshot = nested_proxy_storage->getStorageSnapshot(nested_proxy_storage->getInMemoryMetadataPtr(), context);
        columns = underlying_storage_snapshot->metadata->getColumns();
        data_stream_semantic = nested_proxy_storage->dataStreamSemantic();
    }
    else if (subquery)
    {
        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
            subquery->children[0], context, SelectQueryOptions().subquery().analyze().removeDuplicates());

        auto source_header = interpreter_subquery->getSampleBlock();
        columns = ColumnsDescription(source_header.getNamesAndTypesList());

        /// determine whether it is a streaming query
        streaming = interpreter_subquery->isStreamingQuery();
        data_stream_semantic = interpreter_subquery->getDataStreamSemantic();
    }
    else
    {
        assert(storage);

        if (storage->as<StorageView>())
        {
            underlying_storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
            auto select = underlying_storage_snapshot->getMetadataForQuery()->getSelectQuery().inner_query;

            auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(select, context, SelectQueryOptions{}.analyze());
            auto source_header = interpreter_subquery->getSampleBlock();
            columns = ColumnsDescription(source_header.getNamesAndTypesList());

            /// determine whether it is a streaming query
            streaming = interpreter_subquery->isStreamingQuery();
            data_stream_semantic = interpreter_subquery->getDataStreamSemantic();
        }
        else if (storage->supportsStreamingQuery())
        {
            underlying_storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
            columns = underlying_storage_snapshot->metadata->getColumns();
            data_stream_semantic = storage->dataStreamSemantic();
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Doesn't support apply {} to storage '{}'", getName(), storage->getName());
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
        context,
        streaming_func_desc,
        timestamp_func_desc,
        nested_proxy_storage,
        getName(),
        storage,
        subquery,
        data_stream_semantic,
        streaming);
}

ColumnsDescription TableFunctionProxyBase::getActualTableStructure(ContextPtr /* context */) const
{
    return columns;
}

TableFunctionDescriptionMutablePtr TableFunctionProxyBase::createStreamingTableFunctionDescription(ASTPtr ast, ContextPtr context) const
{
    auto & func = ast->as<ASTFunction &>();
    auto syntax_analyzer_result
        = TreeRewriter(context).analyze(func.arguments, columns.getAll(), nested_proxy_storage ? nested_proxy_storage : storage, underlying_storage_snapshot);

    ExpressionAnalyzer func_expr_analyzer(func.arguments, syntax_analyzer_result, context);
    auto expr_before_table_function = func_expr_analyzer.getActions(true);

    const auto & args_header = expr_before_table_function->getSampleBlock();

    /// Loop actions to figure out input argument types
    Names argument_names;
    argument_names.reserve(args_header.columns());

    DataTypes argument_types;
    argument_types.reserve(args_header.columns());

    for (const auto & column_with_type : args_header)
    {
        argument_names.emplace_back(column_with_type.name);
        argument_types.emplace_back(column_with_type.type);
    }

    auto additional_result_columns = getAdditionalResultColumns(args_header.getColumnsWithTypeAndName());

    return std::make_shared<TableFunctionDescription>(
        std::move(ast),
        toWindowType(func.name),
        argument_names,
        argument_types,
        std::move(expr_before_table_function),
        syntax_analyzer_result->requiredSourceColumns(),
        std::move(additional_result_columns));
}
}
}
