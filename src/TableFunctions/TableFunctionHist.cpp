#include "TableFunctionHist.h"

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>
#include <Storages/StorageView.h>
#include <Storages/Streaming/storageUtil.h>
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
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// table(table_id)
    auto * node = func_ast->as<ASTFunction>();
    ASTs & args = node->arguments->children;
    if (args.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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
                "table function only can be applied to subquery on non-aggreagtion query '{}' ",
                storage_id.getNameForLogs());

        columns = ColumnsDescription{interpreter.getSampleBlock().getNamesAndTypesList()};
        return nullptr;
    }
    else
    {
        assert(storage);
        if (auto * view = storage->as<StorageView>())
        {
            InterpreterSelectWithUnionQuery interpreter(
                view->getInMemoryMetadataPtr()->getSelectQuery().inner_query, context, SelectQueryOptions().subquery().analyze());
            if (interpreter.hasAggregation())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "table function only can be applied to view on non-aggreagtion query '{}'",
                    storage_id.getNameForLogs());
        }
        else if (!supportStreamingQuery(storage))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "table function can't be applied to {} '{}'", storage->getName(), storage_id.getNameForLogs());

        underlying_storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
        columns = underlying_storage_snapshot->getMetadataForQuery()->getColumns();
        return storage;
    }
}

void registerTableFunctionHist(TableFunctionFactory & factory)
{
    factory.registerFunction(
        "table",
        []() -> TableFunctionPtr { return std::make_shared<TableFunctionHist>("table"); },
        {},
        TableFunctionFactory::CaseSensitive,
        /*support subquery*/ true);
}
}
}
