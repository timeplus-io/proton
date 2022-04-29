#include "StreamingWindowCommon.h"

#include <Functions/Streaming/FunctionsStreamingWindow.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
    extern const int MISSING_SESSION_KEY;
}

namespace
{
    std::optional<IntervalKind> mapIntervalKind(const String & func_name)
    {
        if (func_name == "to_interval_second")
            return IntervalKind::Second;
        else if (func_name == "to_interval_minute")
            return IntervalKind::Minute;
        else if (func_name == "to_interval_hour")
            return IntervalKind::Hour;
        else if (func_name == "to_interval_day")
            return IntervalKind::Day;
        else if (func_name == "to_interval_week")
            return IntervalKind::Week;
        else if (func_name == "to_interval_month")
            return IntervalKind::Month;
        else if (func_name == "to_interval_quarter")
            return IntervalKind::Quarter;
        else if (func_name == "to_interval_year")
            return IntervalKind::Year;
        else
            return {};
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

WindowType toWindowType(const String & func_name)
{
    WindowType type = WindowType::NONE;
    if (func_name == ProtonConsts::HOP_FUNC_NAME)
        type = WindowType::HOP;
    else if (func_name == ProtonConsts::TUMBLE_FUNC_NAME)
        type = WindowType::TUMBLE;
    else if (func_name == ProtonConsts::SESSION_FUNC_NAME)
        type = WindowType::SESSION;

    return type;
}

ALWAYS_INLINE bool isTableFunctionTumble(const ASTFunction * ast)
{
    assert(ast);
    return !strcasecmp("tumble", ast->name.c_str());
}

ALWAYS_INLINE bool isTableFunctionHop(const ASTFunction * ast)
{
    assert(ast);
    return !strcasecmp("hop", ast->name.c_str());
}

ALWAYS_INLINE bool isTableFunctionSession(const ASTFunction * ast)
{
    assert(ast);
    return !strcasecmp("session", ast->name.c_str());
}

ALWAYS_INLINE bool isTableFunctionTable(const ASTFunction * ast)
{
    assert(ast);
    return !strcasecmp("table", ast->name.c_str());
}

ASTs checkAndExtractTumbleArguments(const ASTFunction * func_ast)
{
    assert(isTableFunctionTumble(func_ast));

    /// tumble(table, [timestamp_expr], win_interval, [timezone])
    if (func_ast->children.size() != 1)
        throw Exception(HOP_HELP_MESSAGE, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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
        throw Exception(HOP_HELP_MESSAGE, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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

ASTs checkAndExtractSessionArguments(const ASTFunction * func_ast)
{
    assert(isTableFunctionSession(func_ast));

    /// session(table, [timestamp_expr], timeout_interval, [key_column1, key_column2, ...])
    if (func_ast->children.size() != 1)
        throw Exception(SESSION_HELP_MESSAGE, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto & args = func_ast->arguments->children;
    if (args.size() < 3)
        throw Exception(SESSION_HELP_MESSAGE, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

    ASTs asts;
    ASTPtr table;
    ASTPtr time_expr;
    ASTPtr session_interval;
    bool has_time_column = true;

    do
    {
        table = args[0];

        if (isTimeExprAST(args[1]) && isIntervalAST(args[2]))
        {
            /// Case: session(stream, timestamp, INTERVAL 5 SECOND, ...)
            time_expr = args[1];
            session_interval = args[2];
            if (args.size() == 3)
                throw Exception("session(stream, ...) requires at least one session key column, provides zero", ErrorCodes::MISSING_SESSION_KEY); /// throw error, missing session key
        }
        else if (isIntervalAST(args[1]))
        {
            time_expr = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME);
            session_interval = args[1];
            has_time_column = false;
        }
        else
            break; /// throw error

        asts.emplace_back(table);
        asts.emplace_back(time_expr);
        asts.emplace_back(session_interval);
        asts.insert(asts.end(), std::next(args.begin(), has_time_column ? 3 : 2), args.end());
        return asts;

    } while (false);

    throw Exception(SESSION_HELP_MESSAGE, ErrorCodes::BAD_ARGUMENTS);
}

void checkIntervalAST(const ASTPtr & ast, const String & msg)
{
    if (!isIntervalAST(ast))
        throw Exception(msg, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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

ASTPtr makeASTInterval(Int64 num_units, IntervalKind kind)
{
    return makeASTFunction(
        kind.toNameOfFunctionToIntervalDataType(), std::make_shared<ASTLiteral>(num_units < 0 ? Int64(num_units) : UInt64(num_units)));
}

ASTPtr makeASTInterval(const std::pair<Int64, IntervalKind> & interval)
{
    return makeASTInterval(interval.first, interval.second);
}

void convertToSameKindIntervalAST(const BaseScaleInterval & bs1, const BaseScaleInterval & bs2, ASTPtr & ast1, ASTPtr & ast2)
{
    if (bs1.src_kind < bs2.src_kind)
        ast2 = makeASTInterval(bs2.toIntervalKind(bs1.src_kind));
    else if (bs1.src_kind > bs2.src_kind)
        ast1 = makeASTInterval(bs1.toIntervalKind(bs2.src_kind));
}

std::pair<Int64, IntervalKind> BaseScaleInterval::toIntervalKind(IntervalKind::Kind to_kind) const
{
    if (scale == to_kind)
        return {num_units, to_kind};

    const auto & bs = toBaseScale(1, to_kind);
    if (scale != bs.scale)
        throw Exception(
            ErrorCodes::CANNOT_CONVERT_TYPE,
            "Scale conversion is not possible between '{}' and '{}'",
            IntervalKind(src_kind).toString(),
            IntervalKind(to_kind).toString());

    return {num_units / bs.num_units, to_kind};
}

String BaseScaleInterval::toString() const
{
    return fmt::format("{}{}", num_units, (scale == SCALE_SECOND ? "s" : "M"));
}
}
