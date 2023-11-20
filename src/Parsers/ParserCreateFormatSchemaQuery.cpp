#include <Parsers/ASTCreateFormatSchemaQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateFormatSchemaQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>

namespace DB
{

bool ParserCreateFormatSchemaQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_format_schema("FORMAT SCHEMA");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_on("ON");
    ParserIdentifier schema_name_p;
    ParserKeyword s_as("AS");
    ParserStringLiteral schema_src_p;
    ParserKeyword s_schema_type("TYPE");
    String schema_type;

    ASTPtr schema_name;
    ASTPtr schema_core;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected, false))
        or_replace = true;

    if (!s_format_schema.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!schema_name_p.parse(pos, schema_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (!s_as.ignore(pos, expected))
        return false;

    if (!schema_src_p.parse(pos, schema_core, expected))
        return false;

    if (!s_schema_type.ignore(pos, expected))
        return false;

    if (!parseIdentifierOrStringLiteral(pos, expected, schema_type))
        return false;

    auto create_format_schema_query = std::make_shared<ASTCreateFormatSchemaQuery>();
    node = create_format_schema_query;

    create_format_schema_query->schema_name = schema_name;
    create_format_schema_query->children.push_back(schema_name);

    create_format_schema_query->schema_core = schema_core;
    create_format_schema_query->children.push_back(schema_core);

    create_format_schema_query->or_replace = or_replace;
    create_format_schema_query->if_not_exists = if_not_exists;
    create_format_schema_query->cluster = std::move(cluster_str);

    create_format_schema_query->schema_type = std::move(schema_type);

    return true;
}

}
