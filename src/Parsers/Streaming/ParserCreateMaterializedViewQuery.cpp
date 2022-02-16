#include "ParserCreateMaterializedViewQuery.h"

#include <IO/ReadHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{
bool ParserCreateMaterializedViewQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserCompoundIdentifier table_name_p(true);
    ParserKeyword s_as("AS");
    ParserKeyword s_streaming_view("MATERIALIZED VIEW");

    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserTablePropertiesDeclarationList table_properties_p;
    ParserSelectWithUnionQuery select_p;

    ASTPtr table;
    ASTPtr to_table;
    ASTPtr to_inner_uuid;
    ASTPtr columns_list;
    ASTPtr select;

    String cluster_str;
    bool attach = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
    {
        if (s_attach.ignore(pos, expected))
            attach = true;
        else
            return false;
    }

    if (!s_streaming_view.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;

    /// Optional - target table
    if (ParserKeyword{"INTO INNER UUID"}.ignore(pos, expected))
    {
        ParserStringLiteral literal_p;
        if (!literal_p.parse(pos, to_inner_uuid, expected))
            return false;
    }
    else if (ParserKeyword{"INTO"}.ignore(pos, expected))
    {
        // INTO [db.]table
        if (!table_name_p.parse(pos, to_table, expected))
            return false;
    }

    /// Optional - a list of columns can be specified. It must fully comply with SELECT.
    if (s_lparen.ignore(pos, expected))
    {
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    /// AS SELECT ...
    if (!s_as.ignore(pos, expected))
        return false;

    if (!select_p.parse(pos, select, expected))
        return false;

    /// Comment
    ParserKeyword s_comment("COMMENT");
    ParserStringLiteral string_literal_parser;
    ASTPtr comment;
    s_comment.ignore(pos, expected) && string_literal_parser.parse(pos, comment, expected);

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_materialized_view = true;

    const auto & table_identifier = table->as<ASTTableIdentifier &>();
    query->setDatabase(table_identifier.getDatabaseName());
    query->setTable(table_identifier.shortName());
    query->uuid = table_identifier.uuid;
    query->cluster = cluster_str;

    if (to_table)
        query->to_table_id = to_table->as<ASTTableIdentifier>()->getTableId();

    if (to_inner_uuid)
        query->to_inner_uuid = parseFromString<UUID>(to_inner_uuid->as<ASTLiteral>()->value.get<String>());

    query->set(query->columns_list, columns_list);
    if (comment)
        query->set(query->comment, comment);

    query->set(query->select, select);

    return true;
}

}
