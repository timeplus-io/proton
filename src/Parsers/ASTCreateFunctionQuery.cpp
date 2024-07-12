#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

/// proton: starts
#include <Parsers/formatAST.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTLiteral.h>

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
    res->payload = payload;
    return res;
}

void ASTCreateFunctionQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        settings.ostr << "OR REPLACE ";

    /// proton: starts
    if (is_aggregation)
        settings.ostr << "AGGREGATE FUNCTION ";
    else
        settings.ostr << "FUNCTION ";
    /// proton: ends

    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getFunctionName()) << (settings.hilite ? hilite_none : "");

    /// proton: starts
    bool is_javascript_func = isJavaScript();
    bool is_remote = isRemote();
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
        settings.ostr << (settings.hilite ? hilite_keyword : "") << fmt::format("\nTYPE Remote \n") << (settings.hilite ? hilite_none : "");
        settings.ostr << fmt::format("URL '{}'\n", payload->get("AUTH_METHOD").toString());
        settings.ostr << fmt::format("AUTH_METHOD '{}'\n", payload->has("AUTH_METHOD") ? payload->get("AUTH_METHOD").toString() : "none");
        settings.ostr << fmt::format("AUTH_HEADER '{}'\n", payload->has("AUTH_HEADER") ? payload->get("AUTH_HEADER").toString() : "none");
        settings.ostr << fmt::format("AUTH_KEY '{}'\n", payload->has("AUTH_KEY") ? payload->get("AUTH_KEY").toString() : "none");
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

    /// remote functio
    if (is_remote)
    {
        inner_func->set("url", payload->get("URL").toString());
        // auth
        if (payload->has("AUTH_METHOD"))
        {
            inner_func->set("auth_method", payload->get("AUTH_METHOD").toString());
            Poco::JSON::Object::Ptr auth_context = new Poco::JSON::Object();
            auth_context->set("key_name", payload->get("AUTH_HEADER").toString());
            auth_context->set("key_value", payload->get("AUTH_KEY").toString());
            inner_func->set("auth_context", auth_context);
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
