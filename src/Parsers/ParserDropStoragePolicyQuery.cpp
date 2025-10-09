#include <Parsers/ASTDropStoragePolicyQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParser.h>
#include <Parsers/ParserDropStoragePolicyQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>

namespace DB
{

bool ParserDropStoragePolicyQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_storage_policy("STORAGE POLICY");
    ParserKeyword s_if_exists("IF EXISTS");
    /// proton: starts
    /// ParserKeyword s_on("ON");
    /// proton: ends

    String policy_name;

    String cluster_str;
    bool if_exists = false;

    ASTPtr disk_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_storage_policy.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!parseIdentifierOrStringLiteral(pos, expected, policy_name))
        return false;

    /// proton: starts
    /// if (s_on.ignore(pos, expected))
    /// {
    ///    if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
    ///        return false;
    /// }
    /// proton: ends

    auto drop_disk_query = std::make_shared<ASTDropStoragePolicyQuery>();
    drop_disk_query->name = policy_name;
    drop_disk_query->if_exists = if_exists;
    drop_disk_query->cluster = std::move(cluster_str);

    node = drop_disk_query;

    return true;
}

}
