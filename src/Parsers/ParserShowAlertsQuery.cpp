#include <Parsers/ParserShowAlertsQuery.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTShowAlertsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
bool ParserShowAlertsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    if (!ParserKeyword{"SHOW"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"ALERTS"}.ignore(pos, expected))
        return false;

    ASTPtr database_ast;
    if (ParserKeyword{"FROM"}.ignore(pos, expected))
    {
        if (!ParserIdentifier{}.parse(pos, database_ast, expected))
            return false;
    }

    auto query = std::make_shared<ASTShowAlertsQuery>();
    if (database_ast)
    {
        String database;
        tryGetIdentifierNameInto(database_ast, database);
        query->database = {std::move(database)};
    }

    node = query;
    return true;
}
}
