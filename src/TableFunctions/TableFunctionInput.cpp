#include <TableFunctions/TableFunctionInput.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageInput.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <boost/algorithm/string.hpp>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionInput::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        /// proton: starts
        throw Exception("Function '" + getName() + "' must have arguments", ErrorCodes::LOGICAL_ERROR);
        /// proton: ends

    auto args = function->arguments->children;

    if (args.size() != 1)
        /// proton: starts
        throw Exception("Function '" + getName() + "' requires exactly 1 argument: structure",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        /// proton: ends

    structure = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context), "structure");
}

ColumnsDescription TableFunctionInput::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionInput::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto storage = StorageInput::create(StorageID(getDatabaseName(), table_name), getActualTableStructure(context));
    storage->startup();
    return storage;
}

void registerTableFunctionInput(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionInput>();
}

}
