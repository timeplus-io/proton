#include <Parsers/ParserDropAlertQuery.h>

#include <Parsers/ASTDropAlertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserDropAlertQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    if (!ParserKeyword{"DROP"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"ALERT"}.ignore(pos, expected))
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

    auto drop_alert_query = std::make_shared<ASTDropAlertQuery>();
    if (database)
    {
        drop_alert_query->database = database;
        drop_alert_query->children.push_back(database);
    }
    drop_alert_query->name = name;
    drop_alert_query->children.push_back(name);
    drop_alert_query->if_exists = if_exists;

    node = drop_alert_query;
    return true;
}

}
