#include <Parsers/ParserShowDisksQuery.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTShowDisksQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{
bool ParserShowDisksQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_disks("DISKS");
    ParserKeyword s_in("IN");
    ParserKeyword s_not("NOT");
    ParserKeyword s_like("LIKE");
    ParserKeyword s_ilike("ILIKE");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_limit("LIMIT");
    ParserStringLiteral like_p;
    ParserIdentifier name_p;
    ParserExpressionWithOptionalAlias exp_elem(false);

    ASTPtr like;

    auto query = std::make_shared<ASTShowDisksQuery>();

    if (!s_show.ignore(pos, expected))
        return false;

    if (!s_disks.ignore(pos, expected))
        return false;

    if (s_not.ignore(pos, expected))
        query->not_like = true;

    if (bool insensitive = s_ilike.ignore(pos, expected); insensitive || s_like.ignore(pos, expected))
    {
        if (insensitive)
            query->case_insensitive_like = true;

        if (!like_p.parse(pos, like, expected))
            return false;
    }
    else if (query->not_like)
        return false;
    else if (s_where.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, query->where_expression, expected))
            return false;
    }

    if (s_limit.ignore(pos, expected))
    {
        if (!exp_elem.parse(pos, query->limit_length, expected))
            return false;
    }

    if (like)
        query->like = like->as<ASTLiteral &>().value.safeGet<const String &>();

    node = query;

    return true;
}
}
