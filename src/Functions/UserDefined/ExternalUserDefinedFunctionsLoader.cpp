#include "ExternalUserDefinedFunctionsLoader.h"

#include <boost/algorithm/string/split.hpp>

#include <DataTypes/DataTypeFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunction.h>
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>

/// proton: starts
#include <Functions/UserDefined/UDFHelper.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FUNCTION_ALREADY_EXISTS;
    /// proton: starts.
    extern const int UNSUPPORTED_METHOD;
    /// proton: ends
}

ExternalUserDefinedFunctionsLoader::ExternalUserDefinedFunctionsLoader(ContextPtr global_context_)
    : ExternalLoader("external user defined function", &Poco::Logger::get("ExternalUserDefinedFunctionsLoader"))
    , WithContext(global_context_)
{
    if (!global_context_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Passing nullptr to init UDF loader");

    setConfigSettings({"function", "name", "database", "uuid"});
    enableAsyncLoading(false);
    enablePeriodicUpdates(true);
    enableAlwaysLoadEverything(true);
}

ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr
ExternalUserDefinedFunctionsLoader::getUserDefinedFunction(const std::string & user_defined_function_name) const
{
    return std::static_pointer_cast<const UserDefinedExecutableFunction>(load(user_defined_function_name));
}

ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr
ExternalUserDefinedFunctionsLoader::tryGetUserDefinedFunction(const std::string & user_defined_function_name) const
{
    return std::static_pointer_cast<const UserDefinedExecutableFunction>(tryLoad(user_defined_function_name));
}

void ExternalUserDefinedFunctionsLoader::reloadFunction(const std::string & user_defined_function_name) const
{
    loadOrReload(user_defined_function_name);
}

/// proton: starts
ExternalUserDefinedFunctionsLoader & ExternalUserDefinedFunctionsLoader::instance(ContextPtr context)
{
    static ExternalUserDefinedFunctionsLoader loader{context};
    return loader;
}
/// proton: ends

ExternalLoader::LoadablePtr ExternalUserDefinedFunctionsLoader::create(const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & key_in_config,
    const std::string &) const
{
    if (FunctionFactory::instance().hasBuiltInNameOrAlias(name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", name);

    if (AggregateFunctionFactory::instance().hasBuiltInNameOrAlias(name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", name);

    /// proton: starts
    return Streaming::createUserDefinedExecutableFunction(getContext(), name, config);
    /// proton: ends
}

}
