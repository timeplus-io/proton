#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageNull.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionNull.h>
#include <Interpreters/evaluateConstantExpression.h>
#include "registerTableFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionNull::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        /// proton: starts
        throw Exception("Function '" + getName() + "' requires 'structure'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        /// proton: ends

    const auto & arguments = function->arguments->children;
    if (arguments.size() != 1)
        /// proton: starts
        throw Exception("Function '" + getName() + "' requires 'structure'.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        /// proton: ends

    structure = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(arguments[0], context), "structure");
}

ColumnsDescription TableFunctionNull::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionNull::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto res = StorageNull::create(StorageID(getDatabaseName(), table_name), columns, ConstraintsDescription(), String{});
    res->startup();
    return res;
}

void registerTableFunctionNull(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNull>();
}
}
