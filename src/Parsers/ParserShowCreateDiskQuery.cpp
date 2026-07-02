#include <Parsers/ParserShowCreateDiskQuery.h>

#include <Parsers/ASTShowCreateDiskQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
bool ParserShowCreateDiskQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, bool)
{
    ParserKeyword s_show("SHOW");
    ParserKeyword s_create("CREATE");
    ParserKeyword s_disk("DISK");
    ParserIdentifier name_p(true);

    if (!s_show.ignore(pos, expected) || !s_create.ignore(pos, expected) || !s_disk.ignore(pos, expected))
        return false;

    ASTPtr disk_name;
    if (!name_p.parse(pos, disk_name, expected))
        return false;

    auto query = std::make_shared<ASTShowCreateDiskQuery>();
    query->disk_name = disk_name;
    query->children.push_back(disk_name);
    node = query;
    return true;
}
}
