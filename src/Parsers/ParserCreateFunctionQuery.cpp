#include <Parsers/ParserCreateFunctionQuery.h>

#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>

/// proton: starts
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/Streaming/ParserArguments.h>
/// proton: ends


namespace DB
{

bool ParserCreateFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_function("FUNCTION");

    /// proton: starts
    ParserKeyword s_aggr_function("AGGREGATE FUNCTION");
    ParserKeyword s_returns("RETURNS");
    ParserKeyword s_javascript_type("LANGUAGE JAVASCRIPT");
#ifdef ENABLE_PYTHON_UDF
    ParserKeyword s_python_type("LANGUAGE PYTHON");
#endif
    [[maybe_unused]] ParserKeyword s_settings("SETTINGS");
    ParserArguments arguments_p;
    ParserDataType return_p;
    ParserStringLiteral js_src_p;
    [[maybe_unused]] ParserSetQuery settings_p(/* parse_only_internals_ = */ true);

    /// proton: ends

    ParserKeyword s_or_replace("OR REPLACE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_on("ON");
    ParserIdentifier function_name_p;
    ParserKeyword s_as("AS");
    ParserLambdaExpression lambda_p;

    ASTPtr function_name;
    ASTPtr function_core;

    /// proton: starts
    ASTPtr arguments;
    ASTPtr return_type;
    ASTPtr settings;
    bool is_aggregation = false;
    bool is_javascript_func = false;
    bool is_python_func = false;

    bool is_new_syntax = false;
    /// proton: ends

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected, false))
        or_replace = true;

    /// proton: starts
    if (!s_function.ignore(pos, expected))
    {
        if(!s_aggr_function.ignore(pos, expected))
            return false;

        is_aggregation = true;
    }
    /// proton: ends

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    /// proton: starts
    if (arguments_p.parse(pos, arguments, expected))
        is_new_syntax = true;
    /// proton: ends

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    /// proton: starts
    if (is_new_syntax && s_returns.ignore(pos, expected))
    {
        if(!return_p.parse(pos, return_type, expected))
            return false;

        if (s_javascript_type.ignore(pos, expected))
            is_javascript_func = true;
#ifdef ENABLE_PYTHON_UDF
        if (s_python_type.ignore(pos, expected))
            is_python_func = true;
#endif

        if (!s_as.ignore(pos, expected))
            return false;

        /// Parse source code and function_core will be 'ASTLiteral'
        if ((is_javascript_func || is_python_func) && !js_src_p.parse(pos, function_core, expected))
            return false;
        
        if (s_settings.ignore(pos, expected))
        {
            if (!settings_p.parse(pos, settings, expected))
                return false;
        }
    }
    else
    {
        /// SQL function
        if (!s_as.ignore(pos, expected))
            return false;

        if (!lambda_p.parse(pos, function_core, expected))
            return false;
    }
    /// proton: ends

    auto create_function_query = std::make_shared<ASTCreateFunctionQuery>();
    node = create_function_query;

    create_function_query->function_name = function_name;
    create_function_query->children.push_back(function_name);

    create_function_query->function_core = function_core;
    create_function_query->children.push_back(function_core);

    create_function_query->or_replace = or_replace;
    create_function_query->if_not_exists = if_not_exists;
    create_function_query->cluster = std::move(cluster_str);

    /// proton: starts
    create_function_query->is_aggregation = is_aggregation;
    if (is_javascript_func)
        create_function_query->lang = ASTCreateFunctionQuery::Language::JavaScript;
    else if (is_python_func)
        create_function_query->lang = ASTCreateFunctionQuery::Language::Python;
    else
        create_function_query->lang = ASTCreateFunctionQuery::Language::SQL;
    create_function_query->arguments = std::move(arguments);
    create_function_query->return_type = std::move(return_type);
    create_function_query->udf_settings = settings;
    /// proton: ends

    return true;
}

}
