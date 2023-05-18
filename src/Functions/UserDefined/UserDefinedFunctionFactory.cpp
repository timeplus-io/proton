#include "UserDefinedFunctionFactory.h"

/// proton: starts
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionJavaScriptAdapter.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/ExternalUserDefinedFunctionsLoader.h>
#include <Functions/UserDefined/ExecutableUserDefinedFunction.h>
#include <Functions/UserDefined/RemoteUserDefinedFunction.h>
#include <Functions/UserDefined/JavaScriptUserDefinedFunction.h>
#include <Interpreters/Context.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
/// proton: starts
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
/// proton: ends
}

UserDefinedFunctionFactory & UserDefinedFunctionFactory::instance()
{
    static UserDefinedFunctionFactory result;
    return result;
}

FunctionOverloadResolverPtr UserDefinedFunctionFactory::get(const String & function_name, ContextPtr context)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(loader.load(function_name));
    const auto & configuration = executable_function->getConfiguration();
    switch (configuration->type)
    {
        case UserDefinedFunctionConfiguration::FuncType::EXECUTABLE: {
            auto function = std::make_shared<ExecutableUserDefinedFunction>(std::move(executable_function), std::move(context));
            return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
        }
        case UserDefinedFunctionConfiguration::FuncType::REMOTE: {
            auto function = std::make_shared<RemoteUserDefinedFunction>(std::move(executable_function), std::move(context));
            return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
        }
        case UserDefinedFunctionConfiguration::FuncType::JAVASCRIPT: {
            auto function = std::make_shared<JavaScriptUserDefinedFunction>(std::move(executable_function), std::move(context));
            return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
        }
        default:
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "UDF type {} does not support yet.", configuration->type);
    }
}

/// proton: starts
AggregateFunctionPtr UserDefinedFunctionFactory::getAggregateFunction(
    const String & function_name, const DataTypes & types, const Array & parameters, AggregateFunctionProperties & /*properties*/)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(nullptr);
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        const auto * config = executable_function->getConfiguration()->as<JavaScriptUserDefinedFunctionConfiguration>();

        if (!config || !config->is_aggregation)
            return nullptr;

        /// check arguments
        if (types.size() != config->arguments.size())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "expect {} arguments but get {} for UDF {}",
                config->arguments.size(),
                types.size(),
                config->name);

        for (int i = 0; i < config->arguments.size(); i++)
        {
            if (types[i]->getTypeId() != config->arguments[i].type->getTypeId())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "argument {} of UDF {}: expect type '{}' but get '{}'",
                    i,
                    config->name,
                    config->arguments[i].type->getName(),
                    types[i]->getName());
        }

        ContextPtr query_context;
        if (CurrentThread::isInitialized())
            query_context = CurrentThread::get().getQueryContext();

        if (!query_context || !query_context->getSettingsRef().javascript_max_memory_bytes)
        {
            LOG_ERROR(&Poco::Logger::get("UserDefinedFunctionFactory"), "query_context is invalid");
            return nullptr;
        }

        return std::make_shared<AggregateFunctionJavaScriptAdapter>(
            *config, types, parameters, query_context->getSettingsRef().javascript_max_memory_bytes);
    }

    return nullptr;
}

bool UserDefinedFunctionFactory::isAggregateFunctionName(const String & function_name)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(nullptr);
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        const auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        const auto & config = executable_function->getConfiguration();

        return config->is_aggregation;
    }
    return false;
}

bool UserDefinedFunctionFactory::isOrdinaryFunctionName(const String & function_name)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(nullptr);
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        const auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        const auto & config = executable_function->getConfiguration();

        return !config->is_aggregation;
    }
    return false;
}
/// proton: ends

FunctionOverloadResolverPtr UserDefinedFunctionFactory::tryGet(const String & function_name, ContextPtr context)
{
    /// proton: starts
    try
    {
        return get(function_name,std::move(context));
    }
    catch (Exception &)
    {
        return nullptr;
    }
    /// proton: ends
}

bool UserDefinedFunctionFactory::has(const String & function_name, ContextPtr context)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    auto load_result = loader.getLoadResult(function_name);

    bool result = load_result.object != nullptr;
    return result;
}

std::vector<String> UserDefinedFunctionFactory::getRegisteredNames(ContextPtr context)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    auto loaded_objects = loader.getLoadedObjects();

    std::vector<std::string> registered_names;
    registered_names.reserve(loaded_objects.size());

    for (auto & loaded_object : loaded_objects)
        registered_names.emplace_back(loaded_object->getLoadableName());

    return registered_names;
}

}
