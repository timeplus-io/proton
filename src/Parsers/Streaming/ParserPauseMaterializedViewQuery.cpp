#include "ParserPauseMaterializedViewQuery.h"
#include "ASTPauseMaterializedViewQuery.h"
#include "ASTUnpauseMaterializedViewQuery.h"

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace Streaming
{
bool ParserPauseMaterializedViewQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, bool hint)
{
    /// PAUSE MATERIALIZED VIEW 'mv_name'
    ParserKeyword s_pause("PAUSE");
    ParserKeyword s_materialized("MATERIALIZED");
    ParserKeyword s_view("VIEW");

    if (!s_pause.ignore(pos, expected))
        return false;

    if (!s_materialized.ignore(pos, expected))
        return false;

    if (!s_view.ignore(pos, expected))
        return false;

    ASTPtr mvs;
    auto parsed = ParserList(std::make_unique<ParserCompoundIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma))
                      .parse(pos, mvs, expected, hint);
    if (parsed)
    {
        for (auto & elem : mvs->as<ASTExpressionList &>().children)
        {
            auto table_ident = elem->as<ASTIdentifier &>().createTable();
            if (!table_ident)
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR,
                    "PAUSE MATERIALIZED VIEW query requires a list of materialiezd view identifier, for example, PAUSE MATERIALIZED VIEW "
                    "'mv1', 'default.mv2', but got '{}'",
                    elem->formatForErrorMessage());

            elem = table_ident; // Replace ASTIdentifier with ASTTableIdentifier
        }

        auto pause_ast = std::make_shared<ASTPauseMaterializedViewQuery>();
        pause_ast->children.push_back(mvs);
        pause_ast->mvs = mvs;
        node = pause_ast;
    }

    return parsed;
}

bool ParserUnpauseMaterializedViewQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, bool hint)
{
    /// UNPAUSE MATERIALIZED VIEW 'mv_name'
    ParserKeyword s_pause("UNPAUSE");
    ParserKeyword s_materialized("MATERIALIZED");
    ParserKeyword s_view("VIEW");

    if (!s_pause.ignore(pos, expected))
        return false;

    if (!s_materialized.ignore(pos, expected))
        return false;

    if (!s_view.ignore(pos, expected))
        return false;

    ASTPtr mvs;
    auto parsed = ParserList(std::make_unique<ParserCompoundIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma))
                      .parse(pos, mvs, expected, hint);
    if (parsed)
    {
        for (auto & elem : mvs->as<ASTExpressionList &>().children)
        {
            auto table_ident = elem->as<ASTIdentifier &>().createTable();
            if (!table_ident)
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR,
                    "UNPAUSE MATERIALIZED VIEW query requires a list of materialiezd view identifier, for example, UNPAUSE MATERIALIZED "
                    "VIEW "
                    "'mv1', 'default.mv2', but got '{}'",
                    elem->formatForErrorMessage());

            elem = table_ident; // Replace ASTIdentifier with ASTTableIdentifier
        }

        auto unpause_ast = std::make_shared<ASTUnpauseMaterializedViewQuery>();
        unpause_ast->children.push_back(mvs);
        unpause_ast->mvs = mvs;
        node = unpause_ast;
    }

    return parsed;
}

}
}
