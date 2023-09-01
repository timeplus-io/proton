#include "EventPredicateVisitor.h"

#include <Functions/FunctionsConversion.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <NativeLog/Record/Record.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
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

namespace Streaming
{
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
    {
        Int64 sn = value.get<Int64>();
        if (sn >= 0)
            return sn;
    }

    throw Exception(
        ErrorCodes::UNEXPECTED_EXPRESSION,
        "The event sequence id predicate requrie a constant number or expression greater than or equal to 0");
}

Int64 evaluateConstantSeekTo(SeekBy seek_by, ASTPtr & ast, ContextPtr context)
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
}

SeekToInfoPtr tryParseAndCheckSeekToEarliestInfo(const ASTFunction & predicate_func, SeekBy seek_by, bool left_is_event_col)
{
    if (seek_by != SeekBy::EventTime)
        return nullptr;

    if (left_is_event_col)
    {
        if (auto * right_func = predicate_func.arguments->children[1]->as<ASTFunction>();
            right_func && right_func->name == "earliest_timestamp")
        {
            /// Only support `_tp_time > earliest_timestamp()` or `_tp_time >= earliest_ts()`
            if (predicate_func.name == "greater" || predicate_func.name == "greater_or_equals")
                return std::make_shared<SeekToInfo>("earliest", std::vector<Int64>{nlog::EARLIEST_SN}, SeekToType::SEQUENCE_NUMBER);
            else
                throw Exception(
                    ErrorCodes::UNEXPECTED_EXPRESSION, "Invalid event time predicate '{}'", predicate_func.formatForErrorMessage());
        }
    }
    else
    {
        if (auto * left_func = predicate_func.arguments->children[0]->as<ASTFunction>();
            left_func && left_func->name == "earliest_timestamp")
        {
            /// Only support `earliest_timestamp() < _tp_time` or `earliest_ts() <= _tp_time`
            if (predicate_func.name == "less" || predicate_func.name == "less_or_equals")
                return std::make_shared<SeekToInfo>("earliest", std::vector<Int64>{nlog::EARLIEST_SN}, SeekToType::SEQUENCE_NUMBER);
            else
                throw Exception(
                    ErrorCodes::UNEXPECTED_EXPRESSION, "Invalid event time predicate '{}'", predicate_func.formatForErrorMessage());
        }
    }

    return nullptr;
}
}

std::pair<size_t, SeekBy> EventPredicateMatcher::Data::parseSeekBy(ASTPtr ast) const
{
    SeekBy seek_by = SeekBy::None;
    if (auto * identifier = ast->as<ASTIdentifier>(); identifier && IdentifierSemantic::getColumnName(*identifier).has_value())
    {
        auto short_name = identifier->shortName();
        if (short_name == ProtonConsts::RESERVED_EVENT_TIME)
            seek_by = SeekBy::EventTime;
        else if (short_name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID)
            seek_by = SeekBy::EventSequenceNumber;
    }

    if (seek_by == SeekBy::None)
        return {0, SeekBy::None};

    auto stream_pos = membership_collector.getIdentsMembership(ast);
    if (!stream_pos.has_value())
        throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION, "Unknown column identifier '{}'", ast->formatForErrorMessage());

    return {stream_pos.value(), seek_by};
}

std::tuple<size_t, SeekBy, Int64, bool> EventPredicateMatcher::Data::parseEventPredicate(ASTPtr left_arg_ast, ASTPtr right_arg_ast) const
{
    auto [left_stream_pos, left_seek_by] = parseSeekBy(left_arg_ast);
    auto [right_stream_pos, right_seek_by] = parseSeekBy(right_arg_ast);

    if (left_seek_by == SeekBy::None && right_seek_by == SeekBy::None)
        return {0, SeekBy::None, 0, false}; /// None

    if (left_seek_by != SeekBy::None && right_seek_by != SeekBy::None)
        throw Exception(
            ErrorCodes::UNEXPECTED_EXPRESSION,
            "Invalid event predicate. Comparison between {} and {} is not supported",
            left_arg_ast->formatForErrorMessage(),
            right_arg_ast->formatForErrorMessage());

    /// Check whether is seek to constant expr
    bool left_is_event_col = left_seek_by != SeekBy::None;
    if (left_is_event_col)
    {
        auto seek_to = evaluateConstantSeekTo(left_seek_by, right_arg_ast, getContext());
        return {left_stream_pos, left_seek_by, seek_to, left_is_event_col};
    }
    else
    {
        auto seek_to = evaluateConstantSeekTo(right_seek_by, left_arg_ast, getContext());
        return {right_stream_pos, right_seek_by, seek_to, left_is_event_col};
    }
}

std::pair<size_t, SeekToInfoPtr> EventPredicateMatcher::Data::parseSeekToInfo(const ASTFunction & func, ASTPtr & ast) const
{
    assert(func.arguments->children.size() == 2);
    auto & left_arg = func.arguments->children[0];
    auto & right_arg = func.arguments->children[1];

    /// We only handle predicates like `_tp_sn >= / <= ...` or ` _tp_time >= / <= ...` etc simple predicates.
    /// For complex predicates like `a_func(_tp_sn) >= / <= ...` are not supported.
    auto [stream_pos, seek_by, seek_point, left_is_event_col] = parseEventPredicate(left_arg, right_arg);

    if (seek_by == SeekBy::None)
        return {};

    /// Special cases: seek to earliest
    if (auto earliset_info = tryParseAndCheckSeekToEarliestInfo(func, seek_by, left_is_event_col))
    {
        /// Optimize this predicate to constant true, e.g. `where _tp_time > earliest_ts()` -> `where true`
        ast = std::make_shared<ASTLiteral>(true);
        return {stream_pos, earliset_info};
    }

    if (left_is_event_col)
    {
        if (seekLeft(func.name))
            /// Case-1: ` _tp_time >= ...`
            return {
                stream_pos,
                std::make_shared<SeekToInfo>(
                    right_arg->getColumnName(),
                    std::vector<Int64>{seek_point},
                    seek_by == SeekBy::EventTime ? SeekToType::ABSOLUTE_TIME : SeekToType::SEQUENCE_NUMBER)};
        else
            /// Case-2: ` _tp_time <= ...`
            return {
                stream_pos, std::make_shared<SeekToInfo>("earliest", std::vector<Int64>{nlog::EARLIEST_SN}, SeekToType::SEQUENCE_NUMBER)};
    }
    else
    {
        if (seekRight(func.name))
            /// Case-3: ` ... <= _tp_time`
            return {
                stream_pos,
                std::make_shared<SeekToInfo>(
                    left_arg->getColumnName(),
                    std::vector<Int64>{seek_point},
                    seek_by == SeekBy::EventTime ? SeekToType::ABSOLUTE_TIME : SeekToType::SEQUENCE_NUMBER)};
        else
            /// Case-4: ` ... >= _tp_time`
            return {
                stream_pos, std::make_shared<SeekToInfo>("earliest", std::vector<Int64>{nlog::EARLIEST_SN}, SeekToType::SEQUENCE_NUMBER)};
    }
}

SeekToInfoPtr EventPredicateMatcher::Data::tryGetSeekToInfo(size_t table_pos) const
{
    if (seek_to_infos.size() > 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "After query optimized, there still are more than 2 streams.");

    auto iter = seek_to_infos.find(table_pos);
    if (iter == seek_to_infos.end())
        return nullptr;

    SeekToInfoPtr res;
    for (auto seek_to_info : iter->second)
    {
        if (!res)
        {
            res = seek_to_info;
            continue;
        }

        /// For event predicate type, seek to priority (low -> high):
        /// 'earliest' -> event time -> absolute sn
        if (res->getSeekToType() == seek_to_info->getSeekToType())
        {
            /// Select info with lowest seek point, for examples:
            /// 1) Both are time:
            ///     `_tp_time` > '2022-10-1' and `_tp_time` > '2000-10-1'   =>  (seek_to=`2000-10-1`)
            /// 2) Both are absolute sn:
            ///     `_tp_sn` > 1 and `_tp_sn` > 2                           =>  (seek_to=1)
            /// 3) Mix absolute sn and 'earliest':
            ///     `_tp_sn` > 1 and `_tp_sn` < 5 (e.g. -2, 'earliest')     =>  (seek_to=1)
            if (static_cast<UInt64>(seek_to_info->getSeekPoints()[0]) < static_cast<UInt64>(res->getSeekPoints()[0]))
                res = seek_to_info;
        }
        else
        {
            /// We allow mixed use of different event predicate types, the event sequence number predicate
            /// dominates `seek_to`, for examples:
            /// `_tp_time` > '2022-10-1' and `_tp_sn` > 1                   =>  (seek_to=1)
            /// 'earliest' and `_tp_time` > '2020-10-1'       =>  (seek_to='2020-10-1')
            if (seek_to_info->getSeekToType() == SeekToType::SEQUENCE_NUMBER || res->getSeekTo() == "earliest")
                res = seek_to_info;
        }
    }

    return res;
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

    if (auto [stream_pos, seek_to_info] = data.parseSeekToInfo(*func, ast); seek_to_info)
        data.seek_to_infos[stream_pos].emplace_back(seek_to_info);
}
}
}
