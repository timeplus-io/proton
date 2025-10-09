#include <Parsers/ParserShowTasksQuery.h>

#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTShowTasksQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
bool ParserShowTasksQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    if (!ParserKeyword{"SHOW"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"TASKS"}.ignore(pos, expected))
        return false;

    ASTPtr database_ast;
    if (ParserKeyword{"FROM"}.ignore(pos, expected) || ParserKeyword{"IN"}.ignore(pos, expected))
    {
        if (!ParserIdentifier{}.parse(pos, database_ast, expected))
            return false;
    }

    auto query = std::make_shared<ASTShowTasksQuery>();
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
