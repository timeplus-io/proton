#include <Parsers/ASTDropDiskQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropDiskQuery.h>

namespace DB
{

bool ParserDropDiskQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_disk("DISK");
    ParserKeyword s_if_exists("IF EXISTS");
    /// proton: starts
    /// ParserKeyword s_on("ON");
    /// proton: ends

    ParserIdentifier disk_name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr disk_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_disk.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!disk_name_p.parse(pos, disk_name, expected))
        return false;

    /// proton: starts
    /// if (s_on.ignore(pos, expected))
    /// {
    ///    if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
    ///        return false;
    /// }
    /// proton: ends

    String name;
    tryGetIdentifierNameInto(disk_name, name);

    auto drop_disk_query = std::make_shared<ASTDropDiskQuery>();
    drop_disk_query->name = name;
    drop_disk_query->if_exists = if_exists;
    drop_disk_query->cluster = std::move(cluster_str);

    node = drop_disk_query;

    return true;
}

}
