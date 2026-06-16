#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/formatAST.h>

/// proton: starts
//#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Access/Common/AccessType.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Functions/UserDefined/UDFHelper.h>
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>
#include <Interpreters/EnsurePython.h>
#include <Parsers/ASTSetQuery.h>
#include <base/defines.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
/// proton: starts
extern const int FUNCTION_ALREADY_EXISTS;
extern const int INVALID_REQUEST;
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED;
/// proton: ends
}

/// proton: starts
namespace
{
struct PythonUDFInitSettings
{
    String init_function_name;
    String init_function_parameters;
    String named_collection;
};

/// Validate the SETTINGS clause syntax for Python UDFs: reject unknown keys and
/// non-string-literal values, then extract the three recognised settings.
/// Semantic invariants (parameter-source rules, named-collection existence and
/// NAMED_COLLECTION access against the creator) are enforced in
/// Streaming::createUserDefinedFunctionRequest, which both the SQL and REST
/// creation paths go through.
PythonUDFInitSettings validatePythonUDFSettings(const ASTPtr & udf_settings)
{
    PythonUDFInitSettings result;
    if (!udf_settings)
        return result;

    const auto * set_query = udf_settings->as<ASTSetQuery>();
    chassert(set_query); /// the parser only ever stores an ASTSetQuery here
    if (!set_query)
        return result;

    auto check_known_key = [](const String & name) {
        if (name != "init_function_name" && name != "init_function_parameters" && name != "named_collection")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown setting '{}' for CREATE FUNCTION, supported settings: init_function_name, init_function_parameters, "
                "named_collection",
                name);
    };

    /// The parser routes `<name> = DEFAULT` into default_settings and `<name> = {param:type}`
    /// into query_parameters rather than changes; neither form carries a string literal, so any
    /// such entry is rejected (unknown key first, otherwise the non-string-literal error).
    if (!set_query->default_settings.empty())
    {
        const auto & name = set_query->default_settings.front();
        check_known_key(name);
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting '{}' for CREATE FUNCTION must be a string literal", name);
    }

    if (!set_query->query_parameters.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unknown setting 'param_{}' for CREATE FUNCTION, supported settings: init_function_name, init_function_parameters, "
            "named_collection",
            set_query->query_parameters.begin()->first);

    for (const auto & change : set_query->changes)
    {
        check_known_key(change.name);

        if (change.value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting '{}' for CREATE FUNCTION must be a string literal", change.name);

        if (change.name == "init_function_name")
            result.init_function_name = change.value.safeGet<String>();
        else if (change.name == "init_function_parameters")
            result.init_function_parameters = change.value.safeGet<String>();
        else
            result.named_collection = change.value.safeGet<String>();
    }

    return result;
}
}
/// proton: ends

BlockIO InterpreterCreateFunctionQuery::execute()
{
    ASTCreateFunctionQuery & create_function_query = query_ptr->as<ASTCreateFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);

    if (create_function_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    auto current_context = getContext();

    current_context->checkAccess(access_rights_elements);

    /// proton: starts. SETTINGS on CREATE FUNCTION is only honored for Python UDFs;
    /// reject it everywhere else instead of silently dropping it.
    if (const auto * set_query = create_function_query.udf_settings ? create_function_query.udf_settings->as<ASTSetQuery>() : nullptr;
        set_query && !set_query->isEmpty() && !create_function_query.isPython())
        throw Exception(ErrorCodes::UNSUPPORTED, "SETTINGS clause in CREATE FUNCTION is only supported for Python UDFs");
    /// proton: ends

    auto function_name = create_function_query.getFunctionName();
    bool throw_if_exists = !create_function_query.if_not_exists && !create_function_query.or_replace;
    bool replace_if_exists = create_function_query.or_replace;

    /// proton: starts. Handle javascript UDF
    if (create_function_query.isJavaScript())
        return handleJavaScriptUDF(throw_if_exists, replace_if_exists);
    else if (create_function_query.isPython())
        return handlePythonUDF(throw_if_exists, replace_if_exists);
    else if (create_function_query.isRemote())
        return handleRemoteUDF(throw_if_exists, replace_if_exists);
    else if (create_function_query.isSQL())
        return handleSQLUDF(throw_if_exists, replace_if_exists);
    /// proton: ends

    return {};
}

/// proton: starts
BlockIO InterpreterCreateFunctionQuery::handleJavaScriptUDF(bool throw_if_exists, bool replace_if_exists)
{
    ASTCreateFunctionQuery & create = query_ptr->as<ASTCreateFunctionQuery &>();
    assert(create.isJavaScript());

    const auto func_name = create.getFunctionName();
    Poco::JSON::Object::Ptr func = create.toJSON();
    UserDefinedFunctionFactory::instance().registerFunction(getContext(), func_name, func, throw_if_exists, replace_if_exists);

    return {};
}

BlockIO InterpreterCreateFunctionQuery::handlePythonUDF(bool throw_if_exists, bool replace_if_exists)
{
    ensurePythonInited();

    ASTCreateFunctionQuery & create = query_ptr->as<ASTCreateFunctionQuery &>();
    assert(create.isPython());

    const auto func_name = create.getFunctionName();
    Poco::JSON::Object::Ptr func = create.toJSON();
    const auto & settings = getContext()->getSettings();
    Poco::JSON::Object::Ptr inner_func = func->getObject("function");
    bool using_numpy = settings.numpy_optimize_enable;
    inner_func->set("numpy_optimize_enable", using_numpy);
    auto init_settings = validatePythonUDFSettings(create.udf_settings);
    /// Propagate each field unconditionally; the cross-field invariants (including
    /// the init_function_name dependency) are enforced at the creation choke point
    /// in Streaming::createUserDefinedFunctionRequest.
    if (!init_settings.init_function_name.empty())
        inner_func->set("init_function_name", init_settings.init_function_name);
    if (!init_settings.init_function_parameters.empty())
        inner_func->set("init_function_parameters", init_settings.init_function_parameters);
    if (!init_settings.named_collection.empty())
        inner_func->set("named_collection", init_settings.named_collection);
    UserDefinedFunctionFactory::instance().registerFunction(getContext(), func_name, func, throw_if_exists, replace_if_exists);

    return {};
}

BlockIO InterpreterCreateFunctionQuery::handleRemoteUDF(bool throw_if_exists, bool replace_if_exists)
{
    ASTCreateFunctionQuery & create = query_ptr->as<ASTCreateFunctionQuery &>();
    assert(create.isRemote());
    const auto func_name = create.getFunctionName();
    Poco::JSON::Object::Ptr func = create.toJSON();
    UserDefinedFunctionFactory::instance().registerFunction(getContext(), func_name, func, throw_if_exists, replace_if_exists);
    return {};
}

BlockIO InterpreterCreateFunctionQuery::handleSQLUDF(bool throw_if_exists, bool replace_if_exists)
{
    ASTCreateFunctionQuery & create = query_ptr->as<ASTCreateFunctionQuery &>();
    assert(create.isSQL());

    const auto func_name = create.getFunctionName();
    Poco::JSON::Object::Ptr func = create.toJSON();
    UserDefinedFunctionFactory::instance().registerFunction(getContext(), func_name, func, throw_if_exists, replace_if_exists);

    return {};
}

/// proton: ends
}
