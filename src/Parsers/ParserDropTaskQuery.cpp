#include <Parsers/ParserDropTaskQuery.h>

#include <Parsers/ASTDropTaskQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

bool ParserDropTaskQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    if (!ParserKeyword{"DROP"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"TASK"}.ignore(pos, expected))
        return false;

    bool if_exists{false};
    if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
        if_exists = true;

    ASTPtr name;
    ASTPtr database;
    ParserIdentifier name_p;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (ParserToken{TokenType::Dot}.ignore(pos, expected))
    {
        database = name;
        if (!name_p.parse(pos, name, expected))
            return false;
    }

    auto drop_task_query = std::make_shared<ASTDropTaskQuery>();
    if (database)
    {
        drop_task_query->database = database;
        drop_task_query->children.push_back(database);
    }
    drop_task_query->name = name;
    drop_task_query->children.push_back(name);
    drop_task_query->if_exists = if_exists;

    node = std::move(drop_task_query);
    return true;
}

}

