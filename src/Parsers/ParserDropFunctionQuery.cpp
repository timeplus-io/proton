#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropFunctionQuery.h>
#include <Parsers/ParserSetQuery.h>

namespace DB
{

bool ParserDropFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_function("FUNCTION");
    ParserKeyword s_if_exists("IF EXISTS");
    /// proton: starts
    /// ParserKeyword s_on("ON");
    /// proton: ends

    ParserIdentifier function_name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr function_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_function.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    /// proton: starts
    /// if (s_on.ignore(pos, expected))
    /// {
    ///     if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
    ///         return false;
    /// }
    /// proton: ends

    /// proton: starts
    ASTPtr settings_ast;
    ParserKeyword s_settings("SETTINGS");

    if (s_settings.ignore(pos, expected))
    {
        ParserSetQuery parser_settings(true);

        if (!parser_settings.parse(pos, settings_ast, expected))
            return false;
    }
    /// proton: ends

    auto drop_function_query = std::make_shared<ASTDropFunctionQuery>();
    drop_function_query->if_exists = if_exists;
    drop_function_query->cluster = std::move(cluster_str);
    /// proton: starts
    drop_function_query->settings_ast = settings_ast;
    /// proton: ends

    node = drop_function_query;

    drop_function_query->function_name = function_name->as<ASTIdentifier &>().name();

    return true;
}

}
