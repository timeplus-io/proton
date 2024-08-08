#include <TableFunctions/TableFunctionTumble.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Streaming/WindowCommon.h>
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
extern const int BAD_ARGUMENTS;
}

namespace Streaming
{
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
    /// __tumble(timestamp_expr, win_interval, [timezone])
    assert(args.size() == 3);

    //// [timezone]
    /// Prune the empty timezone if user doesn't specify one
    if (!args.back())
        args.pop_back();
}

void registerTableFunctionTumble(TableFunctionFactory & factory)
{
    factory.registerFunction(
        "tumble",
        []() -> TableFunctionPtr { return std::make_shared<TableFunctionTumble>("tumble"); },
        {},
        TableFunctionFactory::CaseSensitive,
        /*support subquery*/ true);
}
}
}
