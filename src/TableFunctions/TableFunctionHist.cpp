#include <TableFunctions/TableFunctionHist.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/StorageView.h>
#include <Storages/storageUtil.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Streaming
{
TableFunctionHist::TableFunctionHist(const String & name_) : TableFunctionProxyBase(name_)
{
    help_message = fmt::format("Function '{}' requires only 1 stream parameter", name);
}

void TableFunctionHist::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    if (func_ast->children.size() != 1)
        throw Exception::createRuntime(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, help_message);

    /// table(table_id)
    auto * node = func_ast->as<ASTFunction>();
    ASTs & args = node->arguments->children;
    if (args.size() != 1)
        throw Exception::createRuntime(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, help_message);

    /// First argument is expected to be table or table function
    resolveStorageID(args[0], context);

    /// Calculate column description
    calculateColumnDescriptions(std::move(context));
}

StoragePtr TableFunctionHist::calculateColumnDescriptions(ContextPtr context)
{
    streaming = false;

    if (subquery)
    {
        InterpreterSelectWithUnionQuery interpreter(subquery->children[0], context, SelectQueryOptions().subquery().analyze());
        if (interpreter.hasAggregation())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "table function only can be applied to subquery on non-aggregation query '{}' ",
                storage_id.getNameForLogs());

        columns = ColumnsDescription{interpreter.getSampleBlock().getNamesAndTypesList()};
        data_stream_semantic = interpreter.getDataStreamSemantic();
        return nullptr;
    }
    else
    {
        assert(storage);
        if (auto * view = storage->as<StorageView>())
        {
            InterpreterSelectWithUnionQuery interpreter(
                view->getInMemoryMetadataPtr()->getSelectQuery().inner_query->clone(),
                context,
                SelectQueryOptions().subquery().analyze());
            if (interpreter.hasAggregation())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "table function only can be applied to view on non-aggregation query '{}'",
                    storage_id.getNameForLogs());
        }
        else if (!storage->supportsStreamingQuery())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "table function can't be applied to {} '{}'", storage->getName(), storage_id.getFullTableName());

        /// Use the target table's metadata because columns are always read from the target table of the materialized view
        if (auto * mv = storage->as<StorageMaterializedView>())
            underlying_storage_snapshot = mv->getStorageSnapshot(mv->getTargetInMemoryMetadataPtr(), context);
        else
            underlying_storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);

        columns = underlying_storage_snapshot->getMetadataForQuery()->getColumns();
        data_stream_semantic = storage->dataStreamSemantic();
        return storage;
    }
}

void registerTableFunctionHist(TableFunctionFactory & factory)
{
    factory.registerFunction(
        "table",
        TableFunctionFactoryData{
            []() -> TableFunctionPtr { return std::make_shared<TableFunctionHist>("table"); },
            {},
        },
        TableFunctionFactory::CaseSensitive,
        /*support_subquery=*/true);
}
}
}
