#include <Parsers/ParserCreateAlertQuery.h>

#include <Interpreters/Streaming/WindowCommon.h>
#include <Parsers/ASTCreateAlertQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/Streaming/ParserIntervalAliasExpression.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseIntervalKind.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/IntervalKind.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

bool ParserCreateAlertQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, bool hint)
{
    bool or_replace = false;
    bool if_not_exists = false;

    if (!ParserKeyword{"CREATE"}.ignore(pos, expected))
        return false;

    if (ParserKeyword{"OR REPLACE"}.ignore(pos, expected, false))
        or_replace = true;

    if (!ParserKeyword{"ALERT"}.ignore(pos, expected))
        return false;

    if (!or_replace && ParserKeyword{"IF NOT EXISTS"}.ignore(pos, expected))
        if_not_exists = true;

    ParserIdentifier name_p{};
    ASTPtr database;
    ASTPtr alert_name;
    if (!name_p.parse(pos, alert_name, expected))
        return false;

    if (ParserToken{TokenType::Dot}.ignore(pos, expected))
    {
        database = alert_name;
        if (!name_p.parse(pos, alert_name, expected))
            return false;
    }

    /// to parse
    /// * to_interval_xxx functions
    /// * INTERVAL operator
    /// * interval alias, such as 1m, 2s, etc.
    auto parse_interval = [&pos, &expected, hint](Int64 & interval, IntervalKind::Kind & kind, const String & help_msg) {
        ASTPtr interval_ast;
        ParserIntervalOperatorExpression io_p;
        ParserFunction fun_p;

        if (!fun_p.parse(pos, interval_ast, expected, hint) && !io_p.parse(pos, interval_ast, expected, hint))
            return false;

        try
        {
            Streaming::checkIntervalAST(interval_ast);
            Streaming::extractInterval(interval_ast->as<ASTFunction>(), interval, kind);
        }
        catch (Exception & ex)
        {
            ex.addMessage(help_msg);
            ex.rethrow();
        }

        return true;
    };

    UInt64 batch_size{0};
    Int64 batch_timeout{0};
    IntervalKind::Kind batch_timeout_unit{IntervalKind::Kind::Second};
    if (ParserKeyword{"BATCH"}.ignore(pos, expected))
    {
        ASTPtr batch_size_ast;
        if (!ParserNumber{}.parse(pos, batch_size_ast, expected, hint))
            return false;

        batch_size = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), batch_size_ast->as<ASTLiteral &>().value);
        if (batch_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Batch size cannot be zero");

        if (!ParserKeyword{"EVENT"}.ignore(pos, expected) && !ParserKeyword{"EVENTS"}.ignore(pos, expected))
            return false;

        if (!ParserKeyword{"WITH TIMEOUT"}.ignore(pos, expected))
            return false;

        if (!parse_interval(batch_timeout, batch_timeout_unit, "Invalid batch timeout"))
            return false;
    }

    UInt64 limit{0};
    Int64 limit_interval{0};
    IntervalKind::Kind limit_interval_kind{IntervalKind::Kind::Second};
    if (ParserKeyword{"LIMIT"}.ignore(pos, expected))
    {
        ASTPtr limit_ast;
        if (!ParserNumber{}.parse(pos, limit_ast, expected, hint))
            return false;

        limit = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), limit_ast->as<ASTLiteral &>().value);
        if (limit == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Limit size cannot be zero");

        if (!ParserKeyword{"ALERT"}.ignore(pos, expected) && !ParserKeyword{"ALERTS"}.ignore(pos, expected))
            return false;

        if (!ParserKeyword{"PER"}.ignore(pos, expected))
            return false;

        if (!parse_interval(limit_interval, limit_interval_kind, "Invalid limit interval"))
            return false;
    }

    if (!ParserKeyword{"CALL"}.ignore(pos, expected))
        return false;

    ASTPtr function_name;
    if (!name_p.parse(pos, function_name, expected))
        return false;

    if (!ParserKeyword{"AS"}.ignore(pos, expected))
        return false;

    ASTPtr select;
    if (!ParserSelectQuery{}.parse(pos, select, expected))
        return false;

    auto query = std::make_shared<ASTCreateAlertQuery>();
    node = query;

    if (database)
    {
        query->database = database;
        query->children.push_back(database);
    }

    query->name = alert_name;
    query->children.push_back(alert_name);

    query->set(query->select, select);

    tryGetIdentifierNameInto(function_name, query->function_name);

    query->or_replace = or_replace;
    query->if_not_exists = if_not_exists;

    query->batch_size = batch_size;
    query->batch_timeout = batch_timeout;
    query->batch_timeout_unit = batch_timeout_unit;

    query->limit_count = limit;
    query->limit_interval = limit_interval;
    query->limit_interval_kind = limit_interval_kind;

    return true;
}

}
