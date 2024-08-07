#include <TableFunctions/TableFunctionHop.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
extern const int BAD_ARGUMENTS;
}

namespace Streaming
{
TableFunctionHop::TableFunctionHop(const String & name_) : TableFunctionWindow(name_)
{
}

void TableFunctionHop::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    doParseArguments(func_ast, context, HOP_HELP_MESSAGE);
}

ASTs TableFunctionHop::checkAndExtractArguments(ASTFunction * node) const
{
    /// hop(table, [timestamp_column], hop_interval, hop_win_interval, [timezone])
    return checkAndExtractHopArguments(node);
}

void TableFunctionHop::postArgs(ASTs & args) const
{
    /// __hop(timestamp_expr, hop_interval, win_interval, [timezone])
    assert(args.size() == 4);

    //// [timezone]
    /// Prune the empty timezone if user doesn't specify one
    if (!args.back())
        args.pop_back();

    /// Try do the same scale conversion of hop_interval and win_interval
    convertToSameKindIntervalAST(
        BaseScaleInterval::toBaseScale(extractInterval(args[1]->as<ASTFunction>())),
        BaseScaleInterval::toBaseScale(extractInterval(args[2]->as<ASTFunction>())),
        args[1],
        args[2]);
}

void registerTableFunctionHop(TableFunctionFactory & factory)
{
    factory.registerFunction(
        "hop",
        []() -> TableFunctionPtr { return std::make_shared<TableFunctionHop>("hop"); },
        {},
        TableFunctionFactory::CaseSensitive,
        /*support subquery*/ true);
}
}
}
