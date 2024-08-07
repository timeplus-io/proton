#include <TableFunctions/TableFunctionSession.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

namespace Streaming
{
TableFunctionSession::TableFunctionSession(const String & name_) : TableFunctionWindow(name_)
{
}

void TableFunctionSession::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    doParseArguments(func_ast, context, SESSION_HELP_MESSAGE);
}

ASTs TableFunctionSession::checkAndExtractArguments(ASTFunction * node) const
{
    /// session(stream, [timestamp_expr], timeout_interval, [max_session_size], [range_comparision])
    /// session(stream, [timestamp_expr], timeout_interval, [max_session_size], [start_cond, end_cond])
    return checkAndExtractSessionArguments(node);
}

void TableFunctionSession::postArgs(ASTs & args) const
{
    /// __session(timestamp_expr, timeout_interval, [max_session_size], [<range_comparision>: start_cond, start_with_inclusion, end_cond, end_with_inclusion])
    assert(args.size() == 7);

    auto timeout_interval = extractInterval(args[1]->as<ASTFunction>());
    /// Set default max_session_size if not provided
    if (!args[2])
       args[2] = makeASTInterval(timeout_interval.interval * ProtonConsts::SESSION_SIZE_MULTIPLIER, timeout_interval.unit);

    /// If range predication is not assigned, any incoming event should be able to start a session window.
    if (!args[3])
    {
        args[3] = std::make_shared<ASTLiteral>(true);
        args[4] = std::make_shared<ASTLiteral>(true);
        args[5] = std::make_shared<ASTLiteral>(false);
        args[6] = std::make_shared<ASTLiteral>(true);
    }
    assert(args[3] && args[4] && args[5] && args[6]);

    /// They may be used in aggregation transform, so set an internal alias for them
    args[3]->setAlias(ProtonConsts::STREAMING_SESSION_START);
    args[5]->setAlias(ProtonConsts::STREAMING_SESSION_END);

    /// Try do the same scale conversion of timeout_interval and max_session_size
    convertToSameKindIntervalAST(
        BaseScaleInterval::toBaseScale(timeout_interval),
        BaseScaleInterval::toBaseScale(extractInterval(args[2]->as<ASTFunction>())),
        args[1],
        args[2]);
}

NamesAndTypesList TableFunctionSession::getAdditionalResultColumns(const ColumnsWithTypeAndName & arguments) const
{
    /// __session(timestamp_expr, timeout_interval, max_session_size, start_cond, start_with_inclusion, end_cond, end_with_inclusion)
    assert(arguments.size() == 7);
    return NamesAndTypesList{
        NameAndTypePair(ProtonConsts::STREAMING_WINDOW_START, arguments[0].type),
        NameAndTypePair(ProtonConsts::STREAMING_WINDOW_END, arguments[0].type),
        NameAndTypePair(ProtonConsts::STREAMING_SESSION_START, arguments[3].type),
        NameAndTypePair(ProtonConsts::STREAMING_SESSION_END, arguments[5].type)};
}

void registerTableFunctionSession(TableFunctionFactory & factory)
{
    factory.registerFunction(
        "session",
        []() -> TableFunctionPtr { return std::make_shared<TableFunctionSession>("session"); },
        {},
        TableFunctionFactory::CaseSensitive,
        /*support subquery*/ true);
}
}
}
