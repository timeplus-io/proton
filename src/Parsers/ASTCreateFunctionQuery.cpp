#include <IO/Operators.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Common/quoteString.h>

/// proton: starts
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/formatAST.h>

#include <boost/algorithm/string/case_conv.hpp>
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

void ASTCreateFunctionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
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

    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getFunctionName()) << (settings.hilite ? hilite_none : "");

    /// proton: starts
    bool is_javascript_func = isJavaScript();
    if (is_javascript_func || is_remote)
    {
        /// arguments
        arguments->formatImpl(settings, state, frame);

        /// return type
        settings.ostr << " RETURNS ";
        return_type->formatImpl(settings, state, frame);
    }
    /// proton: ends

    formatOnCluster(settings);

    /// proton: starts
    if (is_remote)
    {
        settings.ostr << fmt::format("\nURL '{}'\n", function_core->as<ASTLiteral>()->value.safeGet<String>());
        auto auth_method = !function_core->children.empty() ? function_core->children[0]->as<ASTLiteral>()->value.safeGet<String>() : "none";
        settings.ostr << fmt::format("AUTH_METHOD '{}'\n", auth_method);
        if (auth_method != "none")
        {
            settings.ostr << fmt::format(
                "AUTH_HEADER '{}'\n",
                function_core->children[1]->as<ASTLiteral>()->value.safeGet<String>());
            settings.ostr << fmt::format(
                "AUTH_KEY '{}'\n", function_core->children[2]->as<ASTLiteral>()->value.safeGet<String>());
        }
        return;
    }
    /// proton: ends
    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");

    /// proton: starts. Do not format the source of JavaScript UDF
    if (is_javascript_func)
    {
        ASTLiteral * js_src = function_core->as<ASTLiteral>();
        settings.ostr << fmt::format("$$\n{}\n$$", js_src->value.safeGet<String>());
    }
    else
        function_core->formatImpl(settings, state, frame);
    /// proton: starts
}

String ASTCreateFunctionQuery::getFunctionName() const
{
    String name;
    tryGetIdentifierNameInto(function_name, name);
    return name;
}

/// proton: starts
Poco::JSON::Object::Ptr ASTCreateFunctionQuery::toJSON() const
{
    Poco::JSON::Object::Ptr func = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    Poco::JSON::Object::Ptr inner_func = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    inner_func->set("name", getFunctionName());
    bool is_remote = isRemote();
    if (!isJavaScript() && !isRemote())
    {
        WriteBufferFromOwnString source_buf;
        formatAST(*function_core, source_buf, false);
        inner_func->set("source", source_buf.str());
        inner_func->set("type", "sql");
        func->set("function", inner_func);
        return func;
    }

    assert(arguments && !arguments->children.empty());

    Poco::JSON::Array::Ptr json_args = new Poco::JSON::Array(Poco::JSON_PRESERVE_KEY_ORDER);
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
    inner_func->set("arguments", json_args);

    /// type
    auto type = lang;
    boost::to_lower(type);
    inner_func->set("type", type);

    /// is_aggregation
    inner_func->set("is_aggregation", is_aggregation);

    /// return_type
    WriteBufferFromOwnString return_buf;
    formatAST(*return_type, return_buf, false);
    inner_func->set("return_type", return_buf.str());

    /// remote function
    if (is_remote)
    {
        inner_func->set("url", function_core->as<ASTLiteral>()->value.safeGet<String>());
        // auth
        if (!function_core->children.empty()){
            auto auth_method = function_core->children[0]->as<ASTLiteral>()->value.safeGet<String>();
            inner_func->set("auth_method", auth_method);
            if (auth_method == "auth_header")
            {
                Poco::JSON::Object::Ptr auth_context = new Poco::JSON::Object();
                auth_context->set("key_name", function_core->children[1]->as<ASTLiteral>()->value.safeGet<String>());
                auth_context->set("key_value", function_core->children[2]->as<ASTLiteral>()->value.safeGet<String>());
                inner_func->set("auth_context", auth_context);
            }
        }
        func->set("function", inner_func);
        /// Remote function don't have source, return early.
        return func;
    }

    /// source
    ASTLiteral * js_src = function_core->as<ASTLiteral>();
    inner_func->set("source", js_src->value.safeGet<String>());

    func->set("function", inner_func);
    return func;
}
/// proton: ends
}
