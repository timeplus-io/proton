#include <Functions/UserDefined/UserDefinedFunctionFactory.h>

/// proton: starts
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionJavaScriptAdapter.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/ExecutableUserDefinedFunction.h>
#include <Functions/UserDefined/ExternalUserDefinedFunctionsLoader.h>
#include <Functions/UserDefined/JavaScriptUserDefinedFunction.h>
#ifdef ENABLE_PYTHON_UDF
#include <Functions/UserDefined/PythonUserDefinedFunction.h>
#include <AggregateFunctions/AggregateFunctionPythonAdapter.h>
#endif
#include <Functions/UserDefined/RemoteUserDefinedFunction.h>
#include <Functions/UserDefined/UDFHelper.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/Streaming/MetaStoreJSONConfigRepository.h>
#include <V8/Utils.h>

#include <CPython/validatePython.h>
#include <Poco/Util/JSONConfiguration.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
/// proton: starts
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int FUNCTION_ALREADY_EXISTS;
extern const int CANNOT_DROP_FUNCTION;
extern const int UNKNOWN_FUNCTION;
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
#ifdef ENABLE_PYTHON_UDF
        case UserDefinedFunctionConfiguration::FuncType::PYTHON: {
            auto function = std::make_shared<PythonUserDefinedFunction>(std::move(executable_function), std::move(context));
            return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
        }
#endif
        default:
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "UDF type {} does not support yet.", configuration->type);
    }
}

/// proton: starts
AggregateFunctionPtr UserDefinedFunctionFactory::getAggregateFunction(
    const String & function_name,
    const DataTypes & types,
    const Array & parameters,
    AggregateFunctionProperties & /*properties*/,
    bool is_changelog_input)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(nullptr);
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        UserDefinedFunctionConfigurationPtr config = nullptr;
        config = executable_function->getConfiguration();
        if (!config || !config->is_aggregation)
            return nullptr;

        auto validate_arguments = [&](size_t num_args) {
            if (types.size() != num_args)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "expect {} arguments but get {} for UDF {}",
                    config->arguments.size(),
                    types.size(),
                    config->name);

            for (size_t i = 0; i < num_args; i++)
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
        };

        /// check arguments
        size_t num_of_args = config->arguments.size();
        validate_arguments(types.back()->getName() == "int8" ? num_of_args : num_of_args - 1);

        ContextPtr query_context;
        if (CurrentThread::isInitialized())
            query_context = CurrentThread::get().getQueryContext();

        if (!query_context || !query_context->getSettingsRef().javascript_max_memory_bytes)
        {
            LOG_ERROR(&Poco::Logger::get("UserDefinedFunctionFactory"), "query_context is invalid");
            return nullptr;
        }
        if (config->type == UserDefinedFunctionConfiguration::FuncType::JAVASCRIPT)
        {
            return std::make_shared<AggregateFunctionJavaScriptAdapter>(
                std::dynamic_pointer_cast<JavaScriptUserDefinedFunctionConfiguration>(config), types, parameters, is_changelog_input, query_context->getSettingsRef().javascript_max_memory_bytes);
        }
#ifdef ENABLE_PYTHON_UDF
        else if (config->type == UserDefinedFunctionConfiguration::FuncType::PYTHON)
        {
            return std::make_shared<AggregateFunctionPythonAdapter>(
                std::dynamic_pointer_cast<PythonUserDefinedFunctionConfiguration>(config), types, parameters, is_changelog_input);
        }
#endif
        else
            return nullptr;
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

/// proton: starts
bool UserDefinedFunctionFactory::has(const String & function_name, ContextPtr context)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    auto load_result = loader.getLoadResult(function_name);

    bool result = load_result.object != nullptr;
    return result;
}
/// proton: ends

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

/// proton: starts
bool UserDefinedFunctionFactory::registerFunction(
    ContextPtr context,
    const String & function_name,
    Poco::JSON::Object::Ptr json_func,
    bool throw_if_exists,
    bool replace_if_exists)
{
    Streaming::validateUDFName(function_name);

    assert(json_func->has("function"));
    Poco::JSON::Object::Ptr config = json_func->getObject("function");
    assert(config);

    assert(config->has("type"));
    if (config->get("type") == "javascript")
        validateJavaScriptFunction(config);
#ifdef ENABLE_PYTHON_UDF
    else if (config->get("type") == "python")
        validatePythonFunction(config);
#endif

    /// Create the UserDefinedExecutableFunction, it also validates the json configuration and throws exception if invalid
    auto udf = Streaming::createUserDefinedExecutableFunction(context, function_name, Poco::Util::JSONConfiguration(json_func));

    /// Check whether the function already exists.
    if (FunctionFactory::instance().hasBuiltInNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

    if (AggregateFunctionFactory::instance().hasBuiltInNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

    if (UserDefinedSQLFunctionFactory::instance().tryGet(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The same SQL function '{}' already exists", function_name);

    try
    {
        auto * metastore_repo = context->getMetaStoreJSONConfigRepository();
        assert(metastore_repo);
        metastore_repo->save(function_name, json_func);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("save UDF {} to MetaStore failed", function_name));
        throw;
    }

    /// load the function into memory.
    /// TODO: in future, ExternalLoader should support 'save' method to directly update memory and persist to disk
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    loader.tryLoad(function_name);

    return true;
}

void UserDefinedFunctionFactory::validateJavaScriptFunction(Poco::JSON::Object::Ptr config)
{
    const String & function_name = config->getValue<String>("name");
    if (!config->has("source"))
        throw Exception(
            ErrorCodes::FUNCTION_ALREADY_EXISTS, "Missing 'source' property of JavaScript function '{}' already exists", function_name);

    /// check whether the source can be compiled
    if (config->has("is_aggregation") && config->getValue<bool>("is_aggregation"))
    {
        /// UDA V8::validateStatelessFunctionSource(function_name, config->get("source"));
        V8::validateAggregationFunctionSource(function_name, {"initialize", "process", "finalize"}, config->get("source"));

        /// add _tp_delta column as the last argument
        assert(config->has("arguments"));
        assert(config->isArray("arguments"));

        bool has_delta_column = false;
        /// Below is a workaround, because Poco::JSON::Object::getArray cannot work well
        Poco::JSON::Array::Ptr json_arguments = config->get("arguments").extract<Poco::JSON::Array::Ptr>();
        for (size_t i = 0; i < json_arguments->size(); i++)
        {
            const auto & arg = json_arguments->getObject(i);
            if (arg->has("name") && arg->get("name") == "_tp_delta")
            {
                has_delta_column = true;
                break;
            }
        }

        if (!has_delta_column)
        {
            Poco::JSON::Object delta_col;
            delta_col.set("name", "_tp_delta");
            delta_col.set("type", "int8");
            json_arguments->add(delta_col);
            config->set("arguments", json_arguments);
        }
    }
    else
        /// UDF
        V8::validateStatelessFunctionSource(function_name, config->get("source"));
}

#ifdef ENABLE_PYTHON_UDF
void UserDefinedFunctionFactory::validatePythonFunction(Poco::JSON::Object::Ptr config)
{
    const String & function_name = config->getValue<String>("name");
    if (!config->has("source"))
        throw Exception(
            ErrorCodes::FUNCTION_ALREADY_EXISTS, "Missing 'source' property of Python function '{}'", function_name);

    /// check whether the source can be compiled
    if (config->has("is_aggregation") && config->getValue<bool>("is_aggregation"))
    {
        cpython::validateAggregationFunctionSource(config->get("source"), function_name, {"initialize", "process", "finalize"});
        assert(config->has("arguments"));
        assert(config->isArray("arguments"));

        [[maybe_unused]] bool has_delta_column = false;
        Poco::JSON::Array::Ptr json_arguments = config->get("arguments").extract<Poco::JSON::Array::Ptr>();

        for (size_t i = 0; i < json_arguments->size(); i++)
        {
            const auto & arg = json_arguments->getObject(i);
            if (arg->has("name") && arg->get("name") == "_tp_delta")
            {
                has_delta_column = true;
                break;
            }
        }

        if (!has_delta_column)
        {
            Poco::JSON::Object delta_col;
            delta_col.set("name", "_tp_delta");
            delta_col.set("type", "int8");
            json_arguments->add(delta_col);
            config->set("arguments", json_arguments);
        }
    }
    else
        cpython::validateStatelessFunctionSource(config->get("source"), function_name);
}
#endif

bool UserDefinedFunctionFactory::unregisterFunction(
    const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists)
{
    if (FunctionFactory::instance().hasBuiltInNameOrAlias(function_name)
        || AggregateFunctionFactory::instance().hasBuiltInNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (!has(function_name, context))
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", function_name);
        else
            return false;
    }

    auto * metastore_repo = context->getMetaStoreJSONConfigRepository();
    assert(metastore_repo);
    metastore_repo->remove(function_name);

    /// Reload all functions to delete the function from memory
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    loader.loadOrReloadAll();

    return true;
}


/// proton: ends
}
