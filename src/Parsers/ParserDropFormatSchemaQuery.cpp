#include <Parsers/ASTDropFormatSchemaQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropFormatSchemaQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>

namespace DB
{

bool ParserDropFormatSchemaQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_format_schema("FORMAT SCHEMA");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserKeyword s_type("TYPE");
    ParserKeyword s_on("ON");
    ParserIdentifier schema_name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr schema_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_format_schema.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!schema_name_p.parse(pos, schema_name, expected))
        return false;

    String schema_type;
    if (s_type.ignore(pos, expected))
    {
        if (!parseIdentifierOrStringLiteral(pos, expected, schema_type))
            return false;
    }

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto drop_format_schema_query = std::make_shared<ASTDropFormatSchemaQuery>();
    drop_format_schema_query->schema_type = schema_type;
    drop_format_schema_query->if_exists = if_exists;
    drop_format_schema_query->cluster = std::move(cluster_str);

    node = drop_format_schema_query;

    drop_format_schema_query->schema_name = schema_name->as<ASTIdentifier &>().name();

    return true;
}

}
