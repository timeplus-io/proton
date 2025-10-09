#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/formatAST.h>

/// proton: starts
//#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Functions/UserDefined/UDFHelper.h>
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>
#include <Interpreters/EnsurePython.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    /// proton: starts
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int INVALID_REQUEST;
    /// proton: ends
}

BlockIO InterpreterCreateFunctionQuery::execute()
{
    ASTCreateFunctionQuery & create_function_query = query_ptr->as<ASTCreateFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);

    if (create_function_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    auto current_context = getContext();

    current_context->checkAccess(access_rights_elements);

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
