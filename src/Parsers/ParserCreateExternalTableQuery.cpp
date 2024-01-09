#include <Parsers/ASTCreateExternalTableQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateExternalTableQuery.h>
#include <Parsers/ParserSetQuery.h>
#include "Parsers/ASTIdentifier.h"

namespace DB
{

bool DB::ParserCreateExternalTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_external_table("EXTERNAL TABLE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_settings("SETTINGS");

    ParserCompoundIdentifier table_name_p(true, true);
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);

    ASTPtr table;
    ASTPtr settings;

    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (s_external_table.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;

    if (s_settings.ignore(pos, expected))
    {
        if (!settings_p.parse(pos, settings, expected))
            return false;
    }

    auto query = std::make_shared<ASTCreateExternalTableQuery>();
    node = query;

    query->create_or_replace = or_replace;
    query->if_not_exists = if_not_exists;

    auto * table_id = table->as<ASTTableIdentifier>();
    query->database = table_id->getDatabase();
    query->table = table_id->getTable();
    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    query->settings = settings;
    if (query->settings)
        query->children.push_back(query->settings);

    return true;
}

}
