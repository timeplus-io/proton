#include <Parsers/ParserShowCreateTaskQuery.h>

#include <Parsers/ASTShowCreateTaskQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Common/typeid_cast.h>


namespace DB
{
bool ParserShowCreateTaskQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    if (!ParserKeyword{"SHOW"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"CREATE"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"TASK"}.ignore(pos, expected))
        return false;

    ParserIdentifier name_p{/*allow_query_parameter_=*/true};

    ASTPtr database;
    ASTPtr name;
    if (!name_p.parse(pos, name, expected))
        return false;

    if (ParserToken{TokenType::Dot}.ignore(pos, expected))
    {
        database = name;
        if (!name_p.parse(pos, name, expected))
            return false;
    }

    auto query = std::make_shared<ASTShowCreateTaskQuery>();
    if (database)
    {
        query->database = database;
        query->children.push_back(database);
    }
    query->name = name;
    query->children.push_back(name);

    node = query;
    return true;
}
}
