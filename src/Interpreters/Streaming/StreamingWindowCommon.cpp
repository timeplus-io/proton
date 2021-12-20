#include "StreamingWindowCommon.h"

#include <Functions/FunctionsStreamingWindow.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    std::optional<IntervalKind> mapIntervalKind(const String & func_name)
    {
        if (func_name == "toIntervalSecond")
            return IntervalKind::Second;
        else if (func_name == "toIntervalMinute")
            return IntervalKind::Minute;
        else if (func_name == "toIntervalHour")
            return IntervalKind::Hour;
        else if (func_name == "toIntervalDay")
            return IntervalKind::Day;
        else if (func_name == "toIntervalWeek")
            return IntervalKind::Week;
        else if (func_name == "toIntervalMonth")
            return IntervalKind::Month;
        else if (func_name == "toIntervalQuarter")
            return IntervalKind::Quarter;
        else if (func_name == "toIntervalYear")
            return IntervalKind::Year;
        else
            return {};
    }

    ALWAYS_INLINE bool isTableAST(const ASTPtr ast)
    {
        String table_name;
        return tryGetIdentifierNameInto(ast, table_name);
    }

    ALWAYS_INLINE bool isTimeExprAST(const ASTPtr ast)
    {
        /// Assume it is a time or time_expr, we will check it later again
        return (ast->as<ASTIdentifier>() || ast->as<ASTFunction>());
    }

    ALWAYS_INLINE bool isIntervalAST(const ASTPtr ast)
    {
        auto func_node = ast->as<ASTFunction>();
        return (func_node && mapIntervalKind(func_node->name));
    }

    ALWAYS_INLINE bool isTimeZoneAST(const ASTPtr ast) { return (ast->as<ASTLiteral>()); }
}

ALWAYS_INLINE bool isTableFunctionTumble(const ASTFunction * ast)
{
    assert(ast);
    return !strcasecmp("TUMBLE", ast->name.c_str());
}

ALWAYS_INLINE bool isTableFunctionHop(const ASTFunction * ast)
{
    assert(ast);
    return !strcasecmp("HOP", ast->name.c_str());
}

ASTs checkAndExtractTumbleArguments(const ASTFunction * func_ast)
{
    assert(isTableFunctionTumble(func_ast));

    /// tumble(table, [timestamp_expr], win_interval, [timezone])
    if (func_ast->children.size() != 1)
    {
        throw Exception(HOP_HELP_MESSAGE, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
    const auto & args = func_ast->arguments->children;
    if (args.size() < 2)
        throw Exception(TUMBLE_HELP_MESSAGE, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

    if (args.size() > 4)
        throw Exception(TUMBLE_HELP_MESSAGE, ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

    ASTPtr table;
    ASTPtr time_expr;
    ASTPtr win_interval;
    ASTPtr timezone;

    do
    {
        /// First argument is expected to be table name
        if (!isTableAST(args[0]))
            break; /// throw error
        table = args[0];

        if (args.size() == 2)
        {
            /// Case: tumble(table, INTERVAL 5 SECOND)
            if (isIntervalAST(args[1]))
                win_interval = args[1];
            else
                break; /// throw error
        }
        else if (args.size() == 3)
        {
            if (isIntervalAST(args[1]) && isTimeZoneAST(args[2]))
            {
                /// Case: tumble(table, INTERVAL 5 SECOND, timezone)
                win_interval = args[1];
                timezone = args[2];
            }
            else if (isTimeExprAST(args[1]) && isIntervalAST(args[2]))
            {
                /// Case: tumble(table, time_column, INTERVAL 5 SECOND)
                time_expr = args[1];
                win_interval = args[2];
            }
            else
                break; /// throw error
        }
        else
        {
            assert(args.size() == 4);
            if (isTimeExprAST(args[1]) && isIntervalAST(args[2]) && isTimeZoneAST(args[3]))
            {
                /// Case: tumble(table, time_expr, INTERVAL 5 SECOND, timezone)
                time_expr = args[1];
                win_interval = args[2];
                timezone = args[3];
            }
            else
                break; /// throw error
        }

        return {table, time_expr, win_interval, timezone};
    } while (false);

    throw Exception(TUMBLE_HELP_MESSAGE, ErrorCodes::BAD_ARGUMENTS);
}

ASTs checkAndExtractHopArguments(const ASTFunction * func_ast)
{
    assert(isTableFunctionHop(func_ast));

    /// hop(table, [timestamp_expr], hop_interval, win_interval, [timezone])
    if (func_ast->children.size() != 1)
    {
        throw Exception(HOP_HELP_MESSAGE, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
    const auto & args = func_ast->arguments->children;
    if (args.size() < 3)
        throw Exception(HOP_HELP_MESSAGE, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

    if (args.size() > 5)
        throw Exception(HOP_HELP_MESSAGE, ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);


    ASTPtr table;
    ASTPtr time_expr;
    ASTPtr hop_interval;
    ASTPtr win_interval;
    ASTPtr timezone;

    do
    {
        /// First argument is expected to be table name
        if (!isTableAST(args[0]))
            break; /// throw error
        table = args[0];

        if (args.size() == 3)
        {
            /// Case: hop(table, INTERVAL 5 SECOND, INTERVAL 1 MINITUE)
            if (isIntervalAST(args[1]) && isIntervalAST(args[2]))
            {
                hop_interval = args[1];
                win_interval = args[2];
            }
            else
                break; /// throw error
        }
        else if (args.size() == 4)
        {
            if (isIntervalAST(args[1]) && isIntervalAST(args[2]) && isTimeZoneAST(args[3]))
            {
                /// Case: hop(table, INTERVAL 5 SECOND, INTERVAL 1 MINITUE, timezone)
                hop_interval = args[1];
                win_interval = args[2];
                timezone = args[3];
            }
            else if (isTimeExprAST(args[1]) && isIntervalAST(args[2]) && isIntervalAST(args[3]))
            {
                /// Case: hop(table, time_expr, INTERVAL 5 SECOND, INTERVAL 1 MINITUE)
                time_expr = args[1];
                hop_interval = args[2];
                win_interval = args[3];
            }
            else
                break; /// throw error
        }
        else
        {
            assert(args.size() == 5);
            if (isTimeExprAST(args[1]) && isIntervalAST(args[2]) && isIntervalAST(args[3]) && isTimeZoneAST(args[4]))
            {
                /// Case: hop(table, time_column, INTERVAL 5 SECOND, INTERVAL 1 MINITUE, timezone)
                time_expr = args[1];
                hop_interval = args[2];
                win_interval = args[3];
                timezone = args[4];
            }
            else
                break; /// throw error
        }

        return {table, time_expr, hop_interval, win_interval, timezone};
    } while (false);

    throw Exception(HOP_HELP_MESSAGE, ErrorCodes::BAD_ARGUMENTS);
}


void extractInterval(const ASTFunction * ast, Int64 & interval, IntervalKind::Kind & kind)
{
    assert(ast);

    if (auto opt_kind = mapIntervalKind(ast->name); opt_kind)
        kind = opt_kind.value();
    else
        throw Exception("Invalid interval function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    const auto * val = ast->arguments ? ast->arguments->children.front()->as<ASTLiteral>() : nullptr;
    if (!val)
        throw Exception("Invalid interval argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (val->value.getType() == Field::Types::UInt64)
    {
        interval = val->value.safeGet<UInt64>();
    }
    else if (val->value.getType() == Field::Types::Int64)
    {
        interval = val->value.safeGet<Int64>();
    }
    else if (val->value.getType() == Field::Types::String)
    {
        interval = std::stoi(val->value.safeGet<String>());
    }
    else
        throw Exception("Invalid interval argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

void extractInterval(const ASTFunction * ast, Int64 & interval, IntervalKind & kind)
{
    extractInterval(ast, interval, kind.kind);
}

std::pair<Int64, IntervalKind> extractInterval(const ASTFunction * ast)
{
    Int64 interval;
    IntervalKind interval_kind;
    extractInterval(ast, interval, interval_kind.kind);
    return {interval, interval_kind};
}

ALWAYS_INLINE Int64 addTime(Int64 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone)
{
    switch (kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: { \
        return AddTime<IntervalKind::KIND>::execute(time_sec, num_units, time_zone); \
    }
        CASE_WINDOW_KIND(Second)
        CASE_WINDOW_KIND(Minute)
        CASE_WINDOW_KIND(Hour)
        CASE_WINDOW_KIND(Day)
        CASE_WINDOW_KIND(Week)
        CASE_WINDOW_KIND(Month)
        CASE_WINDOW_KIND(Quarter)
        CASE_WINDOW_KIND(Year)
#undef CASE_WINDOW_KIND
    }
    __builtin_unreachable();
}

std::pair<Int64, IntervalKind> secondsToInterval(Int64 time_sec, IntervalKind::Kind to_kind)
{
    Int64 num_units = time_sec / IntervalKind(to_kind).toAvgSeconds();
    if (num_units == 0)
        throw Exception("Failed to convert seconds to interval kind", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return {num_units, to_kind};
}

std::pair<Int64, IntervalKind> secondsToInterval(Int64 time_sec)
{
    /// auto detect to_kind
    const auto & to_kind = IntervalKind::fromAvgSeconds(time_sec);
    Int64 num_units = time_sec / to_kind.toAvgSeconds();
    return {num_units, to_kind};
}

Int64 intervalToSeconds(Int64 num_units, IntervalKind::Kind kind)
{
    return num_units * IntervalKind(kind).toAvgSeconds();
}

ASTPtr makeASTInterval(Int64 num_units, IntervalKind kind)
{
    return makeASTFunction(
        kind.toNameOfFunctionToIntervalDataType(), std::make_shared<ASTLiteral>(num_units < 0 ? Int64(num_units) : UInt64(num_units)));
}
}
