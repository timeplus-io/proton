#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateFunctionQuery.h>

/// proton: starts
//#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Functions/UserDefined/UDFHelper.h>
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    /// proton: starts
    extern const int FUNCTION_ALREADY_EXISTS;
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

    /// proton: starts. Not support clickhouse cluster
    //    if (!create_function_query.cluster.empty())
    //    {
    //        if (current_context->getUserDefinedSQLObjectsLoader().isReplicated())
    //            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because used-defined functions are replicated automatically");
    //
    //        DDLQueryOnClusterParams params;
    //        params.access_to_check = std::move(access_rights_elements);
    //        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    //    }
    /// proton: ends

    current_context->checkAccess(access_rights_elements);

    auto function_name = create_function_query.getFunctionName();
    bool throw_if_exists = !create_function_query.if_not_exists && !create_function_query.or_replace;
    bool replace_if_exists = create_function_query.or_replace;

    /// proton: starts. Handle javascript UDF
    if (create_function_query.isJavaScript())
        return handleJavaScriptUDF(throw_if_exists, replace_if_exists);
    else if (create_function_query.isRemote())
        return handleRemoteUDF(throw_if_exists, replace_if_exists);
    /// proton: ends

    UserDefinedSQLFunctionFactory::instance().registerFunction(current_context, function_name, query_ptr, throw_if_exists, replace_if_exists);

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

BlockIO InterpreterCreateFunctionQuery::handleRemoteUDF(bool throw_if_exists, bool replace_if_exists)
{
    ASTCreateFunctionQuery & create = query_ptr->as<ASTCreateFunctionQuery &>();
    assert(create.isRemote());
    const auto func_name = create.getFunctionName();
    Poco::JSON::Object::Ptr func = create.toJSON();
    UserDefinedFunctionFactory::instance().registerFunction(getContext(), func_name, func, throw_if_exists, replace_if_exists);
    return {};
}
/// proton: ends
}