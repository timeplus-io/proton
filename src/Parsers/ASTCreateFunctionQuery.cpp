#include <IO/Operators.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>

/// proton: starts
#include <optional>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/formatAST.h>

#include <cassert>
#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>
/// proton: ends


namespace DB
{

ASTPtr ASTCreateFunctionQuery::clone() const
{
    auto res = std::make_shared<ASTCreateFunctionQuery>(*this);
    res->children.clear();

    res->function_name = function_name->clone();
    res->children.push_back(res->function_name);

    res->function_core = function_core->clone();
    res->children.push_back(res->function_core);
    return res;
}

void ASTCreateFunctionQuery::formatImpl(
    const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        settings.ostr << "OR REPLACE ";

    /// proton: starts
    bool is_remote = isRemote();
    if (is_aggregation)
        settings.ostr << "AGGREGATE FUNCTION ";
    else if (is_remote)
        settings.ostr << "REMOTE FUNCTION ";
    else
        settings.ostr << "FUNCTION ";
    /// proton: ends

    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getFunctionName())
                  << (settings.hilite ? hilite_none : "");

    /// proton: starts
    bool is_udf = isUDF();
    if (is_udf)
    {
        /// arguments
        arguments->formatImpl(settings, state, frame);

        /// return type
        settings.ostr << " RETURNS ";
        return_type->formatImpl(settings, state, frame);

        if (isJavaScript())
        {
            settings.ostr << " LANGUAGE JAVASCRIPT";
        }
        else if (isPython())
        {
            settings.ostr << " LANGUAGE PYTHON";
        }
    }
    /// proton: ends

    formatOnCluster(settings);

    /// proton: starts
    if (is_remote)
    {
        settings.ostr << ' ';
        function_core->formatImpl(settings, state, frame);
        return;
    }
    /// proton: ends
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");

    /// proton: starts. Do not format the source of UDF
    if (is_udf)
    {
        ASTLiteral * udf_src = function_core->as<ASTLiteral>();
        settings.ostr << fmt::format("$$\n{}\n$$", udf_src->value.safeGet<String>());

        if (udf_settings)
        {
            if (const auto * set_query = udf_settings->as<ASTSetQuery>(); set_query && !set_query->changes.empty())
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " SETTINGS " << (settings.hilite ? hilite_none : "");
                udf_settings->formatImpl(settings, state, frame);
            }
        }
    }
    else
        function_core->formatImpl(settings, state, frame);
    /// proton: ends
}

String ASTCreateFunctionQuery::getFunctionName() const
{
    String name;
    tryGetIdentifierNameInto(function_name, name);
    return name;
}

/// proton: starts
String ASTCreateFunctionQuery::getSource() const
{
    /// source
    ASTLiteral * js_src = function_core->as<ASTLiteral>();
    return js_src->value.safeGet<String>();
}

String ASTCreateFunctionQuery::getReturnType() const
{
    WriteBufferFromOwnString return_buf;
    formatAST(*return_type, return_buf, false);
    return return_buf.str();
}

bool ASTCreateFunctionQuery::isAggregation() const
{
    return is_aggregation;
}

Poco::JSON::Object::Ptr ASTCreateFunctionQuery::toJSON() const
{
    Poco::JSON::Object::Ptr func = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    Poco::JSON::Object::Ptr inner_func = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    inner_func->set("name", getFunctionName());
    if (isSQL())
    {
        WriteBufferFromOwnString query_buf;
        formatAST(*this, query_buf, false);
        inner_func->set("query", query_buf.str());
        inner_func->set("type", "sql");
        func->set("function", inner_func);
        return func;
    }

    Poco::JSON::Array::Ptr json_args = new Poco::JSON::Array(Poco::JSON_PRESERVE_KEY_ORDER);
    if (arguments && !arguments->children.empty())
    {
        for (auto ast : arguments->children[0]->children)
        {
            Poco::JSON::Object::Ptr json_arg = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
            ASTNameTypePair * arg = ast->as<ASTNameTypePair>();
            assert(arg);
            json_arg->set("name", arg->name);
            WriteBufferFromOwnString buf;
            formatAST(*(arg->type), buf, false);
            json_arg->set("type", buf.str());
            json_args->add(json_arg);
        }
    }
    inner_func->set("arguments", json_args);

    /// type
    std::string type_name = std::string(magic_enum::enum_name(lang));
    boost::algorithm::to_lower(type_name);
    inner_func->set("type", type_name);


    /// is_aggregation
    inner_func->set("is_aggregation", is_aggregation);

    /// return_type
    inner_func->set("return_type", getReturnType());

    /// remote function
    if (isRemote())
    {
        assert(function_core != nullptr && function_core->as<ASTExpressionList>());
        auto keyvalue_list = function_core->as<ASTExpressionList>();
        std::optional<String> url;
        std::optional<String> auth_method;
        std::optional<String> auth_header;
        std::optional<String> auth_key;
        std::optional<UInt64> execution_timeout;
        for (ASTPtr child : keyvalue_list->children)
        {
            auto pair = child->as<ASTPair>();
            if (pair != nullptr)
            {
                if (pair->first == "url")
                    url = pair->second->as<ASTLiteral>()->value.safeGet<String>();
                else if (pair->first == "auth_method")
                    auth_method = pair->second->as<ASTLiteral>()->value.safeGet<String>();
                else if (pair->first == "auth_header")
                    auth_header = pair->second->as<ASTLiteral>()->value.safeGet<String>();
                else if (pair->first == "auth_key")
                    auth_key = pair->second->as<ASTLiteral>()->value.safeGet<String>();
                else if (pair->first == "execution_timeout")
                    execution_timeout = pair->second->as<ASTLiteral>()->value.safeGet<UInt64>();
            }
        }
        inner_func->set("url", url.value());
        if (auth_method.has_value())
        {
            inner_func->set("auth_method", auth_method.value());
            if (auth_method.value() == "auth_header")
            {
                Poco::JSON::Object::Ptr auth_context = new Poco::JSON::Object();
                auth_context->set("key_name", auth_header.value_or(""));
                auth_context->set("key_value", auth_key.value_or(""));
                inner_func->set("auth_context", auth_context);
            }
        }
        else
        {
            inner_func->set("auth_method", "none");
        }
        if (execution_timeout.has_value())
        {
            inner_func->set("command_execution_timeout", execution_timeout.value());
        }
        func->set("function", inner_func);
        /// Remote function don't have source, return early.
        return func;
    }
    /// source
    inner_func->set("source", getSource());

    func->set("function", inner_func);
    return func;
}
/// proton: ends
}
