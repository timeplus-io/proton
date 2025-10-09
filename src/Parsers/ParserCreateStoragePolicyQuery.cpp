#include <Parsers/ASTCreateStoragePolicyQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParser.h>
#include <Parsers/ParserCreateStoragePolicyQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>

namespace DB
{

bool DB::ParserCreateStoragePolicyQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_storage_policy("STORAGE POLICY");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_as("AS");

    ParserStringLiteral yaml_config_p;

    String policy_name;
    ASTPtr yaml_config;

    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!s_storage_policy.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!parseIdentifierOrStringLiteral(pos, expected, policy_name))
        return false;

    if (!s_as.ignore(pos, expected))
        return false;

    if (!yaml_config_p.parse(pos, yaml_config, expected))
        return false;

    ASTLiteral * config_ast = yaml_config->as<ASTLiteral>();

    auto create_query = std::make_shared<ASTCreateStoragePolicyQuery>();
    node = create_query;
    create_query->name = policy_name;
    create_query->or_replace = or_replace;
    create_query->if_not_exists = if_not_exists;
    create_query->yaml_config = config_ast->value.safeGet<String>();

    return true;
}

}
