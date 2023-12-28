#include <Parsers/ASTShowFormatSchemasQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ParserShowFormatSchemasQuery.h>


namespace DB
{
bool ParserShowFormatSchemasQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (!ParserKeyword{"SHOW"}.ignore(pos, expected))
        return false;

    if (!ParserKeyword{"FORMAT SCHEMAS"}.ignore(pos, expected))
        return false;

    auto query = std::make_shared<ASTShowFormatSchemasQuery>();

    if (ParserKeyword{"TYPE"}.ignore(pos, expected))
    {
        String schema_type;
        if (!parseIdentifierOrStringLiteral(pos, expected, schema_type))
            return false;
        query->schema_type = schema_type;
    }

    node = query;
    return true;
}
}
