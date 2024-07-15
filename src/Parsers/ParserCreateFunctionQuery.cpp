#include <Parsers/ParserCreateFunctionQuery.h>

#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

/// proton: starts
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/Streaming/ParserArguments.h>

#include <Poco/JSON/Object.h>
/// proton: ends


namespace DB
{

namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_NOT_APPLICABLE;
extern const int UNKNOWN_FUNCTION;
}
bool ParserCreateFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_function("FUNCTION");

    /// proton: starts
    ParserKeyword s_aggr_function("AGGREGATE FUNCTION");
    ParserKeyword s_returns("RETURNS");
    ParserKeyword s_javascript_type("LANGUAGE JAVASCRIPT");
    ParserKeyword s_remote("REMOTE FUNCTION");
    ParserKeyword s_url("URL");
    ParserKeyword s_auth_method("AUTH_METHOD");
    ParserKeyword s_auth_header("AUTH_HEADER");
    ParserKeyword s_auth_key("AUTH_KEY");
    ParserLiteral value;
    ASTPtr url;
    ASTPtr auth_method;
    ASTPtr auth_header;
    ASTPtr auth_key;
    ParserArguments arguments_p;
    ParserDataType return_p;
    ParserStringLiteral js_src_p;
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
    bool is_aggregation = false;
    bool is_javascript_func = false;
    bool is_new_syntax = false;
    bool is_remote = false;
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
        if (s_aggr_function.ignore(pos, expected))
            is_aggregation = true;
        else if (s_remote.ignore(pos, expected))
            is_remote = true;
        else
            return false;
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
        if (!return_p.parse(pos, return_type, expected))
            return false;

        if (s_javascript_type.ignore(pos, expected))
            is_javascript_func = true;

        if (!is_remote && !s_as.ignore(pos, expected))
            return false;

        /// Parse source code and function_core will be 'ASTLiteral'
        if (is_javascript_func && !js_src_p.parse(pos, function_core, expected))
            return false;
    }
    else
    {
        /// SQL function
        if (!s_as.ignore(pos, expected))
            return false;

        if (!lambda_p.parse(pos, function_core, expected))
            return false;
    }
    Poco::JSON::Object::Ptr remote_func_settings = new Poco::JSON::Object();
    if (is_remote)
    {
        if (is_aggregation)
        {
            throw Exception("Remote udf can not be an aggregate function", ErrorCodes::AGGREGATE_FUNCTION_NOT_APPLICABLE);
        }
        if (!s_url.ignore(pos, expected))
            return false;
        if (!value.parse(pos, url, expected))
            return false;
        remote_func_settings->set("URL", url->as<ASTLiteral>()->value.safeGet<String>());
        if (s_auth_method.ignore(pos, expected))
        {
            if (!value.parse(pos, auth_method, expected))
                return false;
            auto method_str = auth_method->as<ASTLiteral>()->value.safeGet<String>();
            if (method_str == "auth_header")
            {
                if (!s_auth_header.ignore(pos, expected))
                    return false;
                if (!value.parse(pos, auth_header, expected))
                    return false;
                if (!s_auth_key.ignore(pos, expected))
                    return false;
                if (!value.parse(pos, auth_key, expected))
                    return false;
                remote_func_settings->set("AUTH_HEADER", auth_header->as<ASTLiteral>()->value.safeGet<String>());
                remote_func_settings->set("AUTH_KEY", auth_key->as<ASTLiteral>()->value.safeGet<String>());
            }
            else if (method_str != "none")
            {
                throw Exception("Auth_method must be 'none' or 'auth_header'", ErrorCodes::UNKNOWN_FUNCTION);
            }
            remote_func_settings->set("AUTH_METHOD", method_str);
        }
        
        function_core = std::make_shared<ASTLiteral>(Field());
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
    create_function_query->lang = is_javascript_func ? "JavaScript" : is_remote ? "Remote" : "SQL";
    create_function_query->arguments = std::move(arguments);
    create_function_query->return_type = std::move(return_type);
    create_function_query->remote_func_settings = remote_func_settings;
    /// proton: ends

    return true;
}

}
