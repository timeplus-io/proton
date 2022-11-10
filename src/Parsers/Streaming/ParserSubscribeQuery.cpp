#include "ParserSubscribeQuery.h"
#include "ASTRecoverQuery.h"
#include "ASTUnsubscribeQuery.h"

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>

namespace DB
{
namespace Streaming
{
bool ParserSubscribeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, bool hint)
{
    ParserKeyword s_subscribe("SUBSCRIBE");
    ParserKeyword s_to("TO");

    if (!s_subscribe.ignore(pos, expected))
        return false;

    if (!s_to.ignore(pos, expected))
        return false;

    ParserSelectWithUnionQuery select_p;
    auto parsed = select_p.parse(pos, node, expected, hint);
    if (parsed)
    {
        auto * select_ast = node->as<ASTSelectWithUnionQuery>();
        assert(select_ast);
        select_ast->exec_mode = SelectExecuteMode::SUBSCRIBE;
    }

    return parsed;
}

bool ParserUnsubscribeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, bool /*hint*/)
{
    /// UNSUBSCRIBE TO 'query-id'
    ParserKeyword s_unsubscribe("UNSUBSCRIBE");
    ParserKeyword s_to("TO");

    if (!s_unsubscribe.ignore(pos, expected))
        return false;

    if (!s_to.ignore(pos, expected))
        return false;

    ParserStringLiteral uuid_p;
    ASTPtr ast_uuid;
    if (!uuid_p.parse(pos, ast_uuid, expected))
        return false;

    auto unsubscribe_ast = std::make_shared<ASTUnsubscribeQuery>();
    unsubscribe_ast->query_id = ast_uuid;
    node = unsubscribe_ast;

    return true;
}

bool ParserRecoverQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, bool /*hint*/)
{
    /// RECOVER FROM 'query-id'
    ParserKeyword s_recover("RECOVER");
    ParserKeyword s_from("FROM");

    if (!s_recover.ignore(pos, expected))
        return false;

    if (!s_from.ignore(pos, expected))
        return false;

    ParserStringLiteral uuid_p;
    ASTPtr ast_uuid;
    if (!uuid_p.parse(pos, ast_uuid, expected))
        return false;

    auto recover_ast = std::make_shared<ASTRecoverQuery>();
    recover_ast->query_id = ast_uuid;
    node = recover_ast;

    return true;
}
}
}
