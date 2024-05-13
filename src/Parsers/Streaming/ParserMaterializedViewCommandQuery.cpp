#include "ParserMaterializedViewCommandQuery.h"
#include "ASTMaterializedViewCommandQuery.h"

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
bool ParserMaterializedViewCommandQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, bool hint)
{
    /// PAUSE|RESUME|ABORT|RECOVER MATERIALIZED VIEW 'mv_name(s)' [SYNC | ASYNC (default)]
    ParserKeyword s_pause("PAUSE");
    ParserKeyword s_resume("RESUME");
    ParserKeyword s_abort("ABORT");
    ParserKeyword s_recover("RECOVER");
    ParserKeyword s_materialized("MATERIALIZED");
    ParserKeyword s_view("VIEW");
    ParserKeyword s_sync("SYNC");
    ParserKeyword s_async("ASYNC");

    MaterializedViewCommandType type = MaterializedViewCommandType::Pause;
    if (s_pause.ignore(pos, expected))
        type = MaterializedViewCommandType::Pause;
    else if (s_resume.ignore(pos, expected))
        type = MaterializedViewCommandType::Resume;
    else if (s_abort.ignore(pos, expected))
        type = MaterializedViewCommandType::Abort;
    else if (s_recover.ignore(pos, expected))
        type = MaterializedViewCommandType::Recover;
    else
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
                    "{} requires a list of materialiezd view identifier, for example, {} MATERIALIZED VIEW 'mv1', 'default.mv2', but got "
                    "'{}'",
                    getName(),
                    Poco::toUpper(magic_enum::enum_name(type)),
                    elem->formatForErrorMessage());

            elem = table_ident; // Replace ASTIdentifier with ASTTableIdentifier
        }

        bool sync = false;
        if (s_sync.ignore(pos, expected))
            sync = true;
        else if (s_async.ignore(pos, expected))
            sync = false;

        auto command_ast = std::make_shared<ASTPauseMaterializedViewQuery>();
        command_ast->type = type;
        command_ast->sync = sync;
        command_ast->children.push_back(mvs);
        command_ast->mvs = mvs;
        node = command_ast;
    }

    return parsed;
}

bool ParserUnpauseMaterializedViewQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, bool hint)
{
    /// UNPAUSE MATERIALIZED VIEW 'mv_name' [SYNC | ASYNC (default)]
    ParserKeyword s_pause("UNPAUSE");
    ParserKeyword s_materialized("MATERIALIZED");
    ParserKeyword s_view("VIEW");
    ParserKeyword s_sync("SYNC");
    ParserKeyword s_async("ASYNC");

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

        bool sync = false;
        if (s_sync.ignore(pos, expected))
            sync = true;
        else if (s_async.ignore(pos, expected))
            sync = false;

        auto unpause_ast = std::make_shared<ASTUnpauseMaterializedViewQuery>();
        unpause_ast->children.push_back(mvs);
        unpause_ast->mvs = mvs;
        unpause_ast->sync = sync;
        node = unpause_ast;
    }

    return parsed;
}

}
}
