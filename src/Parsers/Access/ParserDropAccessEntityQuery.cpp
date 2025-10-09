#include <Parsers/Access/ParserDropAccessEntityQuery.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ParserRowPolicyName.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <base/range.h>


namespace DB
{
namespace
{
    bool parseEntityType(IParserBase::Pos & pos, Expected & expected, AccessEntityType & type)
    {
        for (auto i : collections::range(AccessEntityType::MAX))
        {
            const auto & type_info = AccessEntityTypeInfo::get(i);

            bool hint = !(i == AccessEntityType::SETTINGS_PROFILE || i == AccessEntityType::ROW_POLICY);

            if (ParserKeyword{type_info.name}.ignore(pos, expected, hint)
                || (!type_info.alias.empty() && ParserKeyword{type_info.alias}.ignore(pos, expected, hint)))
            {
                type = i;
                return true;
            }
        }
        return false;
    }


    bool parseOnCluster(IParserBase::Pos & pos [[maybe_unused]], Expected & expected [[maybe_unused]], String & cluster [[maybe_unused]])
    {
        return false;
        /// proton: starts
        /// we don't need to parse ON CLUSTER
        /// return IParserBase::wrapParseImpl(pos, [&]
        /// {
        ///     return ParserKeyword{"ON"}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        /// });
        /// proton: ends
    }
}


bool ParserDropAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (!ParserKeyword{"DROP"}.ignore(pos, expected))
        return false;

    AccessEntityType type;
    if (!parseEntityType(pos, expected, type))
        return false;

    bool if_exists = false;
    if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
        if_exists = true;

    Strings names;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;
    String cluster;

    if ((type == AccessEntityType::USER) || (type == AccessEntityType::ROLE))
    {
        if (!parseUserNames(pos, expected, names))
            return false;
    }
    else if (type == AccessEntityType::ROW_POLICY)
    {
        ParserRowPolicyNames parser;
        ASTPtr ast;
        parser.allowOnCluster();
        if (!parser.parse(pos, ast, expected))
            return false;
        row_policy_names = typeid_cast<std::shared_ptr<ASTRowPolicyNames>>(ast);
        cluster = std::exchange(row_policy_names->cluster, "");
    }
    else
    {
        if (!parseIdentifiersOrStringLiterals(pos, expected, names))
            return false;
    }

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto query = std::make_shared<ASTDropAccessEntityQuery>();
    node = query;

    query->type = type;
    query->if_exists = if_exists;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->row_policy_names = std::move(row_policy_names);

    return true;
}
}
