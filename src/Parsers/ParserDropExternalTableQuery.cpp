#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropExternalTableQuery.h>

namespace DB
{

bool DB::ParserDropExternalTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_external_table("EXTERNAL TABLE");
    ParserKeyword s_if_exists("IF EXISTS");

    ParserCompoundIdentifier table_name_p(true, true);

    ASTPtr table;

    bool if_exists = false;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_external_table.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;

    auto query = std::make_shared<ASTDropQuery>();
    node = query;

    query->kind = ASTDropQuery::Drop;
    query->is_external_table = true;
    query->if_exists = if_exists;

    auto * table_id = table->as<ASTTableIdentifier>();
    query->database = table_id->getDatabase();
    query->table = table_id->getTable();
    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    return true;
}

}
