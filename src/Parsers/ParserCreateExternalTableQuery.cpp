#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateExternalTableQuery.h>
#include <Parsers/ParserCreateQuery.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{
ASTPtr parseComment(IParser::Pos & pos, Expected & expected)
{
    ParserKeyword s_comment("COMMENT");
    ParserStringLiteral string_literal_parser;
    ASTPtr comment;

    s_comment.ignore(pos, expected) && string_literal_parser.parse(pos, comment, expected);

    return comment;
}
}

bool DB::ParserCreateExternalTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_external_table("EXTERNAL TABLE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_as("AS");
    ParserKeyword s_settings("SETTINGS");

    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    ParserTablePropertiesDeclarationList table_properties_p;

    ParserStorage storage_p;
    ParserStringLiteral string_literal_parser;

    ParserCompoundIdentifier table_name_p(true, true);

    ASTPtr table;
    ASTPtr columns_list;
    ASTPtr storage;
    ASTPtr exec_script;

    bool attach = false;
    bool or_replace = false;
    bool if_not_exists = false;

    if (s_create.ignore(pos, expected))
    {
        if (s_or_replace.ignore(pos, expected))
            or_replace = true;
    }
    else if (s_attach.ignore(pos, expected))
        attach = true;
    else
        return false;

    if (!s_external_table.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;

    /// Columns
    if (s_lparen.ignore(pos, expected))
    {
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    if (s_as.ignore(pos, expected))
    {
        if (!string_literal_parser.parse(pos, exec_script, expected))
            return false;
    }

    storage_p.parse(pos, storage, expected);

    if (!storage)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse SETTINGS in CREATE EXTERNAL TABLE.");

    auto * storage_ast = storage->as<ASTStorage>();
    if (storage_ast == nullptr || storage_ast->settings == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SETTINGS is required in CREATE EXTERNAL TABLE.");

    if (storage_ast->engine != nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ENGINE is not allowed in CREATE EXTERNAL TABLE.");

    storage_ast->set(storage_ast->engine, makeASTFunction("ExternalTable"));

    auto comment = parseComment(pos, expected);

    auto create_query = std::make_shared<ASTCreateQuery>();
    node = create_query;

    create_query->type = ASTCreateQuery::Type::ExternalTable;

    /// proton : starts, we don't support replace / create or replace table for now
    // create_query->create_or_replace = or_replace;
    /// proton : ends

    create_query->if_not_exists = if_not_exists;

    auto * table_id = table->as<ASTTableIdentifier>();
    create_query->database = table_id->getDatabase();
    create_query->table = table_id->getTable();
    if (attach)
    {
        create_query->uuid = table_id->uuid;
        create_query->attach = attach;
    }
    if (create_query->database)
        create_query->children.push_back(create_query->database);
    if (create_query->table)
        create_query->children.push_back(create_query->table);

    create_query->set(create_query->columns_list, columns_list);
    create_query->set(create_query->storage, storage);

    if (exec_script)
    {
        const auto * script_literal = exec_script->as<ASTLiteral>();
        if (!script_literal || script_literal->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Inline script table function definition must be a string literal");

        create_query->exec_script = script_literal->value.get<String>();
    }

    if (comment)
        create_query->set(create_query->comment, comment);

    return true;
}

}
