#include <Parsers/CommonParsers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserCreateExternalTableQuery.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{

bool DB::ParserCreateExternalTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_attach("ATTACH");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_external_table("EXTERNAL TABLE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_settings("SETTINGS");

    ParserCompoundIdentifier table_name_p(true, true);
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);

    ASTPtr table;
    ASTPtr settings;

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

    if (s_settings.ignore(pos, expected))
    {
        if (!settings_p.parse(pos, settings, expected))
            return false;
    }

    auto create_query = std::make_shared<ASTCreateQuery>();
    node = create_query;

    create_query->is_external = true;
    create_query->create_or_replace = or_replace;
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

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, makeASTFunction("ExternalTable"));
    storage->set(storage->settings, settings);
    create_query->set(create_query->storage, storage);

    return true;
}

}
