#include "TableFunctionTumble.h"

#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

TableFunctionTumble::TableFunctionTumble(const String & name_) : TableFunctionWindow(name_)
{
}

void TableFunctionTumble::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    doParseArguments(func_ast, context, TUMBLE_HELP_MESSAGE);
}

ASTs TableFunctionTumble::checkAndExtractArguments(ASTFunction * node) const
{
    /// tumble(table, [timestamp_column_expr], win_interval, [timezone])
    return checkAndExtractTumbleArguments(node);
}

void TableFunctionTumble::postArgs(ASTs & args) const
{
    //// [timezone]
    /// Prune the empty timezone if user doesn't specify one
    if (!args.back())
        args.pop_back();
}

String TableFunctionTumble::functionNamePrefix() const
{
    return ProtonConsts::TUMBLE_FUNC_NAME + "(";
}

DataTypePtr TableFunctionTumble::getElementType(const DataTypeTuple * tuple) const
{
    return tuple->getElements()[0];
}

void registerTableFunctionTumble(TableFunctionFactory & factory)
{
    factory.registerFunction("tumble", []() -> TableFunctionPtr { return std::make_shared<TableFunctionTumble>("tumble"); });
}
}
