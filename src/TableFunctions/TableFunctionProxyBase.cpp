#include "TableFunctionProxyBase.h"

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
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
            streaming = stream_storage->isStreaming();
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
    if (subquery)
    {
        auto interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
            subquery->children[0], context, SelectQueryOptions().subquery().analyze());
        auto source_header = interpreter_subquery->getSampleBlock();
        columns = ColumnsDescription(source_header.getNamesAndTypesList());

        /// determine whether it is a streaming query
        streaming = interpreter_subquery->isStreaming();
    }
    else
    {
        assert(storage);
        if (!supportStreamingQuery(storage))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Doesn't support apply {} to storage '{}'", getName(), storage->getName());

        if (storage->as<StorageView>())
        {
            underlying_storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
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
            underlying_storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
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
        context,
        streaming_func_desc,
        timestamp_func_desc,
        nested_proxy_storage,
        getName(),
        storage,
        subquery,
        streaming);
}

ColumnsDescription TableFunctionProxyBase::getActualTableStructure(ContextPtr /* context */) const
{
    return columns;
}

FunctionDescriptionPtr TableFunctionProxyBase::createStreamingFunctionDescription(
    ASTPtr ast, TreeRewriterResultPtr syntax_analyzer_result, ContextPtr context, const String & func_name_prefix) const
{
    ExpressionAnalyzer func_expr_analyzer(ast, syntax_analyzer_result, context);
    auto streaming_func_expr = func_expr_analyzer.getActions(true);

    WindowType type = toWindowType(ast->as<ASTFunction>()->name);

    const auto & actions = streaming_func_expr->getActions();

    /// Loop actions to figure out input argument types
    for (const auto & action : actions)
    {
        if (action.node->type == ActionsDAG::ActionType::FUNCTION && action.node->result_name.starts_with(func_name_prefix))
        {
            Names argument_names;
            argument_names.reserve(action.node->children.size());

            DataTypes argument_types;
            argument_types.reserve(action.node->children.size());

            for (const auto * node : action.node->children)
            {
                argument_names.push_back(node->result_name);
                argument_types.push_back(node->result_type);
            }
            return std::make_shared<FunctionDescription>(
                std::move(ast),
                type,
                argument_names,
                argument_types,
                std::move(streaming_func_expr),
                syntax_analyzer_result->requiredSourceColumns());
        }
    }

    /// The timestamp function ends up with const column, like toDateTime('2020-01-01 00:00:00') or now('UTC') or now64(3, 'UTC')
    /// Check the function name is now or now64 since these are the only const function we support
    const auto & func_name = ast->as<ASTFunction>()->name;
    if (func_name != "now" && func_name != "now64")
        throw Exception("Unsupported const timestamp func for timestamp column", ErrorCodes::BAD_ARGUMENTS);

    /// Parse the argument names
    return std::make_shared<FunctionDescription>(
        std::move(ast),
        type,
        Names{},
        DataTypes{},
        std::move(streaming_func_expr),
        syntax_analyzer_result->requiredSourceColumns(),
        true);
}
}
}
