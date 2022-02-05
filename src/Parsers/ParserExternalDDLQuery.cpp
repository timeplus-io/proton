#include <Parsers/ASTExternalDDLQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserExternalDDLQuery.h>

namespace DB
{
bool ParserExternalDDLQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserFunction p_function;
    ParserKeyword s_external("EXTERNAL DDL FROM");

    ASTPtr from;
    auto external_ddl_query = std::make_shared<ASTExternalDDLQuery>();

    if (!s_external.ignore(pos, expected))
        return false;

    if (!p_function.parse(pos, from, expected))
        return false;

    external_ddl_query->set(external_ddl_query->from, from);

    bool res = false;
    node = external_ddl_query;
    return res;
}

}
