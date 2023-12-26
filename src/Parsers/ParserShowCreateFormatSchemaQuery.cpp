#include <Common/typeid_cast.h>
#include <Parsers/ASTShowCreateFormatSchemaQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserShowCreateFormatSchemaQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


namespace DB
{
bool ParserShowCreateFormatSchemaQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_create("CREATE");
    ParserKeyword s_format("FORMAT");
    ParserKeyword s_schema("SCHEMA");
    ParserKeyword s_type("TYPE");
    ParserIdentifier name_p(true);

    ASTPtr schema;
    String type;
    std::shared_ptr<ASTShowCreateFormatSchemaQuery> query;

    if (!s_show.ignore(pos, expected))
        return false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_format.ignore(pos, expected))
        return false;

    if (!s_schema.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, schema, expected))
        return false;

    if (s_type.ignore(pos, expected))
        if (!parseIdentifierOrStringLiteral(pos, expected, type))
            return false;

    query = std::make_shared<ASTShowCreateFormatSchemaQuery>();
    query->schema_name = schema;
    query->children.push_back(schema);
    query->schema_type = type;

    node = query;

    return true;
}
}
