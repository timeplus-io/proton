#include <Parsers/ASTShowCreateFunctionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserShowCreateFunctionQuery.h>

namespace DB
{
bool ParserShowCreateFunctionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_create("CREATE");
    ParserKeyword s_func("FUNCTION");
    ParserIdentifier name_p(true);

    ASTPtr func;
    std::shared_ptr<ASTShowCreateFunctionQuery> query;

    if (!s_show.ignore(pos, expected))
    {
        return false;
    }

    if (!s_create.ignore(pos, expected))
    {
        return false;
    }
    if (!s_func.ignore(pos, expected))
    {
        return false;
    }

    if (!name_p.parse(pos, func, expected))
    {
        return false;
    }

    query = std::make_shared<ASTShowCreateFunctionQuery>();
    query->function_name = func;
    query->children.push_back(func);

    node = query;

    return true;
}
}
