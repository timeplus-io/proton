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
#include <Parsers/ParserKeyValuePairsSet.h>

#include <Poco/JSON/Object.h>
/// proton: ends


namespace DB
{

/// proton: starts
namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_NOT_APPLICABLE;
extern const int UNKNOWN_FUNCTION;
}
/// proton: ends
bool ParserCreateFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
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
    ParserKeyword s_execution_timeout("EXECUTION_TIMEOUT");
    ParserLiteral value;
    ASTPtr kv_list;
    ASTPtr url;
    ASTPtr auth_method;
    ASTPtr auth_header;
    ASTPtr auth_key;
    ASTPtr execution_timeout;
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
    if (is_remote)
    {
        if (is_aggregation)
        {
            throw Exception("Remote udf can not be an aggregate function", ErrorCodes::AGGREGATE_FUNCTION_NOT_APPLICABLE);
        }
        ParserKeyValuePairsSet kv_pairs_list;
        if (!kv_pairs_list.parse(pos, kv_list, expected))
            return false;
        
        /// check if the parameters are valid and no unsupported or unknown parameters.
        std::optional<String> ast_url;
        std::optional<String> ast_auth_method;
        std::optional<String> ast_auth_header;
        std::optional<String> ast_auth_key;
        std::optional<UInt64> ast_execution_timeout;
        for (const auto & kv : kv_list->children)
        {
            auto * kv_pair = kv->as<ASTPair>();
            auto key = kv_pair->first;
            auto pair_value = kv_pair->second->as<ASTLiteral>()->value;
            if (!kv_pair)
                throw Exception("Key-value pair expected", ErrorCodes::UNKNOWN_FUNCTION);
            
            if (key == "url")
            {
                ast_url = pair_value.safeGet<String>();
            }
            else if (key == "auth_method")
            {
                ast_auth_method = pair_value.safeGet<String>();
                if (ast_auth_method.value() != "none" && ast_auth_method.value() != "auth_header")
                    throw Exception("Unknown auth method", ErrorCodes::UNKNOWN_FUNCTION);
            }
            else if (key == "auth_header")
            {
                ast_auth_header = pair_value.safeGet<String>();
            }
            else if (key == "auth_key")
            {
                ast_auth_key = pair_value.safeGet<String>();
            }
            else if (key == "execution_timeout")
            {
                ast_execution_timeout = pair_value.safeGet<UInt64>();
            }
        }
        /// check if URL is set
        if (!ast_url)
            throw Exception("URL is required for remote function", ErrorCodes::UNKNOWN_FUNCTION);
        /// check if auth_method is "auth_header" or "none"
        if (ast_auth_method)
        {
            if (ast_auth_method.value() == "auth_header")
            {
                if (!ast_auth_header  || !ast_auth_key)
                    throw Exception("Auth header and auth key are required for auth_header auth method", ErrorCodes::UNKNOWN_FUNCTION);
            }
            else if (ast_auth_method.value() == "none")
            {
                if (ast_auth_header || ast_auth_key)
                    throw Exception("Auth method is 'none', but auth header or auth key is set.", ErrorCodes::UNKNOWN_FUNCTION);
            }
            else
            {
                throw Exception("Unknown auth method " + ast_auth_method.value(), ErrorCodes::UNKNOWN_FUNCTION);
            }
        }
        else
        {
            if (ast_auth_header || ast_auth_key)
                throw Exception("Auth method is 'none', but auth header or auth key is set.", ErrorCodes::UNKNOWN_FUNCTION);
        }
        function_core = std::move(kv_list);
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
    /// proton: ends

    return true;
}

}
