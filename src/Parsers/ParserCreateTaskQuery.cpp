#include <Parsers/ParserCreateTaskQuery.h>

#include <Interpreters/Streaming/WindowCommon.h>
#include <Parsers/ASTCreateTaskQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
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

#include <Poco/JSON/Parser.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SYNTAX_ERROR;
}

bool ParserCreateTaskQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, bool hint)
{
    bool or_replace = false;
    bool if_not_exists = false;

    if (!ParserKeyword{"CREATE"}.ignore(pos, expected))
        return false;

    if (ParserKeyword{"OR REPLACE"}.ignore(pos, expected, false))
        or_replace = true;

    if (!ParserKeyword{"TASK"}.ignore(pos, expected))
        return false;

    if (!or_replace && ParserKeyword{"IF NOT EXISTS"}.ignore(pos, expected))
        if_not_exists = true;

    ParserIdentifier name_p{};
    ASTPtr database;
    ASTPtr task_name;
    if (!name_p.parse(pos, task_name, expected))
        return false;

    if (ParserToken{TokenType::Dot}.ignore(pos, expected))
    {
        database = task_name;
        if (!name_p.parse(pos, task_name, expected))
            return false;
    }

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

    if (!ParserKeyword{"SCHEDULE"}.ignore(pos, expected))
        return false;

    ASTPtr schedule_cron;
    Int64 schedule_interval{0};
    IntervalKind::Kind schedule_interval_unit{};

    ParserStringLiteral string_literal_parser;
    if (!string_literal_parser.parse(pos, schedule_cron, expected)
        && !parse_interval(schedule_interval, schedule_interval_unit, "Invalid SCHEDULE interval"))
        return false;

    if (schedule_interval < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schedule interval cannot be less than zero");

    Int64 timeout{0};
    IntervalKind::Kind timeout_unit{};
    if (ParserKeyword{"TIMEOUT"}.ignore(pos, expected))
    {
        if (!parse_interval(timeout, timeout_unit, "Invalid TIMEOUT"))
            return false;

        if (timeout <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Task TIMEOUT value must be greater than zero");
    }

    String checkpoint_str;
    std::vector<String> checkpoint_columns;
    if (ParserKeyword{"CHECKPOINT"}.ignore(pos, expected))
    {
        ASTPtr checkpoint;
        if (!string_literal_parser.parse(pos, checkpoint, expected))
            return false;

        checkpoint_str = checkpoint->as<ASTLiteral>()->value.safeGet<String>();

        Poco::JSON::Parser parser;
        auto obj = parser.parse(checkpoint_str).extract<Poco::JSON::Object::Ptr>();
        if (!obj)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "CHECKPOINT should be in JSON format: actual={}", checkpoint_str);
        auto keys = obj->getNames();
        for (auto & key : keys)
            checkpoint_columns.push_back(std::move(key));
    }

    ASTPtr to_table;
    if (ParserKeyword{"INTO"}.ignore(pos, expected))
    {
        // INTO [db.]table
        ParserCompoundIdentifier table_name_p(true);
        if (!table_name_p.parse(pos, to_table, expected))
            return false;
    }

    if (!ParserKeyword{"AS"}.ignore(pos, expected))
        return false;

    /// The rest is the sql template to execution.
    const auto * sql_begin = pos->begin;
    while (pos.isValid() && pos->type != TokenType::Semicolon)
        ++pos;
    const auto sql_size = static_cast<size_t>(pos->begin - sql_begin);
    String sql{sql_begin, sql_size};
    if (sql.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Task execution <sql> is missing");

    auto query = std::make_shared<ASTCreateTaskQuery>();
    node = query;

    if (database)
    {
        query->database = database;
        query->children.push_back(database);
    }

    query->name = task_name;
    query->children.push_back(task_name);

    query->or_replace = or_replace;
    query->if_not_exists = if_not_exists;

    if (schedule_cron)
    {
        query->cron = schedule_cron->as<ASTLiteral>()->value.safeGet<String>();
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Task cron schedule is not implemented.");
    }
    else
    {
        query->interval = static_cast<UInt64>(schedule_interval);
        query->interval_unit = schedule_interval_unit;
    }

    if (timeout > 0)
    {
        query->timeout = static_cast<UInt64>(timeout);
        query->timeout_unit = timeout_unit;
    }

    if (!checkpoint_columns.empty())
    {
        query->checkpoint_names = std::move(checkpoint_columns);
        query->checkpoint = std::move(checkpoint_str);
    }

    if (to_table)
        query->to_table_id = to_table->as<ASTTableIdentifier>()->getTableId();

    query->sql = std::move(sql);

    return true;
}

}
