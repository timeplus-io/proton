#include <Parsers/ASTCreateDiskQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserCreateDiskQuery.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{

bool DB::ParserCreateDiskQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_disk("DISK");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");

    ParserIdentifier disk_name_p;
    ParserFunction disk_func_p;

    ASTPtr disk_name;
    ASTPtr function_ast;
    Field disk_func_field;

    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!s_disk.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!disk_name_p.parse(pos, disk_name, expected))
        return false;

    if (!disk_func_p.parse(pos, function_ast, expected) || function_ast->as<ASTFunction>()->name != "disk")
        return false;

    auto create_query = std::make_shared<ASTCreateDiskQuery>();
    node = create_query;

    String name;
    tryGetIdentifierNameInto(disk_name, name);
    create_query->name = name;
    create_query->or_replace = or_replace;
    create_query->if_not_exists = if_not_exists;

    create_query->disk_func = function_ast;
    create_query->children.push_back(function_ast);

    return true;
}

}
