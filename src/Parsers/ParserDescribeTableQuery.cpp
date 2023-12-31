#include <Parsers/TablePropertiesQueriesASTs.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDescribeTableQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserDescribeTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_describe("DESCRIBE");
    ParserKeyword s_desc("DESC");
    /// proton : starts.
    ParserKeyword s_table("STREAM");
    /// proton : ends.
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;

    ASTPtr database;
    ASTPtr table;

    if (!s_describe.ignore(pos, expected) && !s_desc.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTDescribeQuery>();

    s_table.ignore(pos, expected);

    ASTPtr table_expression;
    if (!ParserTableExpression().parse(pos, table_expression, expected))
        return false;

    query->table_expression = table_expression;

    node = query;

    return true;
}


}
