#include "EventPredicateVisitor.h"

#include <Functions/FunctionsConversion.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <NativeLog/Record/Record.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNEXPECTED_EXPRESSION;
}

namespace
{
std::unordered_set<String> COMPARISON_FUNCS = {
    "equals", /// = or ==
    "greater", /// >
    "greater_or_equals", /// >=
    "less", /// <
    "less_or_equals", /// <=
    "not_equals", /// <> or !=
};

bool seekLeft(const String & func_name)
{
    return (func_name == "greater_or_equals" || func_name == "equals" || func_name == "greater");
}

bool seekRight(const String & func_name)
{
    return (func_name == "equals" || func_name == "less" || func_name == "less_or_equals");
}

enum class SeekBy : uint8_t
{
    None,
    EventTime,
    EventSequenceNumber
};

Int64 parseSeekToTimestamp(const Field & value, DataTypePtr type)
{
    if (isString(type))
    {
        auto [seek_point, valid] = tryParseAbsoluteTimeSeek(value.safeGet<String>());
        if (!valid)
            throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "The event time predicate requrie a timestamp string");

        return seek_point;
    }

    ToDateTime64Transform to_datetime64(3); /// scale is 3
    if (isDate(type))
        return to_datetime64.execute(UInt16(value.safeGet<UInt16>()), DateLUT::instance("UTC"));
    else if (isDate32(type))
        return to_datetime64.execute(Int32(value.safeGet<Int32>()), DateLUT::instance("UTC"));
    else if (isDateTime(type))
        return to_datetime64.execute(UInt32(value.safeGet<UInt32>()), DateLUT::instance("UTC"));
    else if (isDateTime64(type))
        return value.safeGet<Decimal64>().getValue();
    else
        throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "The event time predicate requrie a constant timestamp string or expression");
}

Int64 parseSeekToSequenceNumber(const Field & value, DataTypePtr type)
{
    if (isNativeInteger(type))
        return value.get<Int64>();
    else
        throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "The event sequence id predicate requrie a constant number or expression");
}

Int64 evaluateConstantSeekTo(SeekBy seek_by, ASTPtr ast, ContextPtr context)
{
    try
    {
        auto [value, type] = evaluateConstantExpression(ast, context);
        if (seek_by == SeekBy::EventTime)
            return parseSeekToTimestamp(value, type);
        else
            return parseSeekToSequenceNumber(value, type);
    }
    catch (const Exception & e)
    {
        /// rethrow with better error message for exception of `evaluateConstantExpression`
        if (e.code() == ErrorCodes::BAD_ARGUMENTS)
        {
            if (seek_by == SeekBy::EventTime)
                throw Exception(
                    ErrorCodes::UNEXPECTED_EXPRESSION,
                    "The event time predicate expression must be constant timestamp string or expression. But got '{}'",
                    ast->formatForErrorMessage());
            else
                throw Exception(
                    ErrorCodes::UNEXPECTED_EXPRESSION,
                    "The event sequence id predicate expression must be constant sequence id expression. But got '{}'",
                    ast->formatForErrorMessage());
        }

        throw;
    }
}

SeekBy parseSeekBy(ASTPtr ast)
{
    if (auto identifier_opt = tryGetIdentifierName(ast); identifier_opt.has_value())
    {
        if (*identifier_opt == ProtonConsts::RESERVED_EVENT_TIME)
            return SeekBy::EventTime;
        else if (*identifier_opt == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID)
            return SeekBy::EventSequenceNumber;
    }
    return SeekBy::None;
}

std::tuple<SeekBy, Int64, bool> parseEventPredicate(ASTPtr left_ast, ASTPtr right_ast, ContextPtr context)
{
    SeekBy left_seek_by = parseSeekBy(left_ast);
    SeekBy right_seek_by = parseSeekBy(right_ast);

    if (left_seek_by == SeekBy::None && right_seek_by == SeekBy::None)
        return {SeekBy::None, 0, false}; /// None

    if (left_seek_by != SeekBy::None && right_seek_by != SeekBy::None)
        throw Exception(
            ErrorCodes::UNEXPECTED_EXPRESSION,
            "Invalid event predicate. Comparison between {} and {} is not supported",
            left_ast->formatForErrorMessage(),
            right_ast->formatForErrorMessage());

    /// Check whether is seek to constant expr
    SeekBy seek_by;
    Int64 seek_to;
    bool left_is_event_col = left_seek_by != SeekBy::None;
    if (left_is_event_col)
    {
        seek_by = left_seek_by;
        seek_to = evaluateConstantSeekTo(seek_by, right_ast, context);
    }
    else
    {
        seek_by = right_seek_by;
        seek_to = evaluateConstantSeekTo(seek_by, left_ast, context);
    }

    return {seek_by, seek_to, left_is_event_col};
}
}

bool EventPredicateMatcher::needChildVisit(ASTPtr & node, ASTPtr & children)
{
    /// For now, only support event predicate in where clause
    if (auto * select = node->as<ASTSelectQuery>())
    {
        if (select->where().get() == children.get())
            return true;
        else
            return false;
    }

    /// Don't descent into table functions and subqueries and special case for ArrayJoin.
    return !(node->as<ASTTableExpression>() || node->as<ASTSubquery>() || node->as<ASTArrayJoin>());
}

void EventPredicateMatcher::visit(ASTPtr & ast, Data & data)
{
    auto * func = ast->as<ASTFunction>();
    if (!func)
        return;

    if (!COMPARISON_FUNCS.contains(func->name))
        return;

    assert(func->arguments->children.size() == 2);
    auto & left_arg = func->arguments->children[0];
    auto & right_arg = func->arguments->children[1];

    /// We only handle predicates like `_tp_sn >= / <= ...` or ` _tp_time >= / <= ...` etc simple predicates.
    /// For complex predicates like `a_func(_tp_sn) >= / <= ...` are not supported.
    auto [seek_by, seek_point, left_is_event_col] = parseEventPredicate(left_arg, right_arg, data.context);

    if (seek_by == SeekBy::None)
        return;

    if (data.event_predicate)
        throw Exception(
            ErrorCodes::UNEXPECTED_EXPRESSION,
            "The event predicate '{}' conflicts with '{}'",
            data.event_predicate->formatForErrorMessage(),
            func->formatForErrorMessage());

    if (left_is_event_col)
    {
        if (seekLeft(func->name))
            data.seek_to_info = std::make_shared<SeekToInfo>(
                right_arg->getColumnName(),
                std::vector<Int64>{seek_point},
                seek_by == SeekBy::EventTime ? SeekToType::ABSOLUTE_TIME : SeekToType::SEQUENCE_NUMBER);
        else
            data.seek_to_info
                = std::make_shared<SeekToInfo>("earliest", std::vector<Int64>{nlog::EARLIEST_SN}, SeekToType::SEQUENCE_NUMBER);
    }
    else
    {
        if (seekRight(func->name))
            data.seek_to_info = std::make_shared<SeekToInfo>(
                left_arg->getColumnName(),
                std::vector<Int64>{seek_point},
                seek_by == SeekBy::EventTime ? SeekToType::ABSOLUTE_TIME : SeekToType::SEQUENCE_NUMBER);
        else
            data.seek_to_info
                = std::make_shared<SeekToInfo>("earliest", std::vector<Int64>{nlog::EARLIEST_SN}, SeekToType::SEQUENCE_NUMBER);
    }

    data.event_predicate = ast;
}

}
