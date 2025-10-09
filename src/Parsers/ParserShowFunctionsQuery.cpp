#include <Parsers/ASTShowFunctionsQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserShowFunctionsQuery.h>

namespace DB
{
bool ParserShowFunctionsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ParserKeyword s_show{"SHOW"};
    ParserKeyword s_functions{"FUNCTIONS"};
    ParserKeyword s_where{"WHERE"};
    ParserExpressionWithOptionalAlias exp_elem(false);

    if (!s_show.ignore(pos, expected))
        return false;
    if (!s_functions.ignore(pos, expected))
        return false;
    auto query = std::make_shared<ASTShowFunctionsQuery>();

    if (s_where.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, query->where_expression, expected))
            return false;
        query->children.emplace_back(query->where_expression);
    }
    node = query;
    return true;
}
}
