#include "ExternalUserDefinedFunctionsLoader.h"

#include <boost/algorithm/string/split.hpp>

#include <DataTypes/DataTypeFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunction.h>
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>

/// proton: starts
#include <Poco/JSON/Parser.h>
#include <Common/filesystemHelpers.h>
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
ExternalUserDefinedFunctionsLoader & ExternalUserDefinedFunctionsLoader::instance(ContextPtr context_)
{
    static ExternalUserDefinedFunctionsLoader loader{context_};
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
    String type = config.getString(key_in_config + ".type");
    UserDefinedFunctionConfiguration::FuncType func_type;
    String command_value;
    String source;
    String command;
    Poco::URI url;
    if (type == "executable")
    {
        func_type = UserDefinedFunctionConfiguration::FuncType::EXECUTABLE;
        command_value = config.getString(key_in_config + ".command", "");
    }
    else if (type == "remote")
    {
        func_type = UserDefinedFunctionConfiguration::FuncType::REMOTE;
        url = Poco::URI(config.getString(key_in_config + ".url"));
    }
    else if (type == "javascript")
    {
        func_type = UserDefinedFunctionConfiguration::FuncType::JAVASCRIPT;
        source = config.getString(key_in_config + ".source", "");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Wrong user defined function type expected 'executable' or 'remote' actual {}",
                        type);
    /// proton: ends

    bool execute_direct = config.getBool(key_in_config + ".execute_direct", true);

    std::vector<String> command_arguments;

    if (execute_direct)
    {
        boost::split(command_arguments, command_value, [](char c) { return c == ' '; });

        command_value = std::move(command_arguments[0]);
        auto user_scripts_path = getContext()->getUserScriptsPath();
        command = std::filesystem::path(user_scripts_path) / command_value;

        if (!fileOrSymlinkPathStartsWith(command, user_scripts_path))
            throw Exception(
                ErrorCodes::UNSUPPORTED_METHOD, "Executable file {} must be inside user scripts folder {}", command_value, user_scripts_path);

        if (!std::filesystem::exists(std::filesystem::path(command)))
            throw Exception(
                ErrorCodes::UNSUPPORTED_METHOD,
                "Executable file {} does not exist inside user scripts folder {}",
                command_value,
                user_scripts_path);

        command_arguments.erase(command_arguments.begin());
    }

    DataTypePtr result_type = DataTypeFactory::instance().get(config.getString(key_in_config + ".return_type"));
    bool send_chunk_header = config.getBool(key_in_config + ".send_chunk_header", false);
    size_t command_termination_timeout_seconds = config.getUInt64(key_in_config + ".command_termination_timeout", 0);
    size_t command_read_timeout_milliseconds = config.getUInt64(key_in_config + ".command_read_timeout", 10000);
    size_t command_write_timeout_milliseconds = config.getUInt64(key_in_config + ".command_write_timeout", 10000);

    size_t pool_size = 0;
    size_t max_command_execution_time = 0;

    max_command_execution_time = config.getUInt64(key_in_config + ".max_command_execution_time", 10);

    size_t max_execution_time_seconds = static_cast<size_t>(getContext()->getSettings().max_execution_time.totalSeconds());
    if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
        max_command_execution_time = max_execution_time_seconds;

    ExternalLoadableLifetime lifetime;

    if (config.has(key_in_config + ".lifetime"))
        lifetime = ExternalLoadableLifetime(config, key_in_config + ".lifetime");

    /// proton: starts
    String format = config.getString(key_in_config + ".format", "ArrowStream");
    bool is_aggr_function = config.getBool(key_in_config + ".is_aggregation", false);
    pool_size = config.getUInt64(key_in_config + ".pool_size", 1);

    /// Below implementation only available for JSON configuration, because Poco::Util::AbstractConfiguration cannot work well with Array
    std::vector<UserDefinedFunctionConfiguration::Argument> arguments;
    String arg_str = config.getRawString(key_in_config + ".arguments", "");
    if (!arg_str.empty())
    {
        Poco::JSON::Parser parser;
        try
        {
            auto json_arguments = parser.parse(arg_str).extract<Poco::JSON::Array::Ptr>();
            for (unsigned int i = 0; i < json_arguments->size(); i++)
            {
                UserDefinedFunctionConfiguration::Argument argument;
                argument.name = json_arguments->getObject(i)->get("name").toString();
                argument.type = DataTypeFactory::instance().get(json_arguments->getObject(i)->get("type").toString());
                arguments.emplace_back(std::move(argument));
            }
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid UDF config");
        }
    }

    /// handler auth_method
    RemoteUserDefinedFunctionConfiguration::AuthMethod auth_method = RemoteUserDefinedFunctionConfiguration::AuthMethod::NONE;
    RemoteUserDefinedFunctionConfiguration::AuthContext auth_ctx;
    String method = config.getString(key_in_config + ".auth_method", "none");
    if (method == "none")
    {
        auth_method = RemoteUserDefinedFunctionConfiguration::AuthMethod::NONE;
    }
    else if (method == "auth_header")
    {
        auth_ctx.key_name = config.getString(key_in_config + ".auth_context.key_name", "");
        auth_ctx.key_value = config.getString(key_in_config + ".auth_context.key_value", "");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Wrong 'auth_method' expected 'none' or 'auth_header' actual {}",
                        method);

    auto init_config = [&](UserDefinedFunctionConfigurationPtr cfg) {
        cfg->type = std::move(func_type);
        cfg->arguments = std::move(arguments);
        cfg->is_aggregation = is_aggr_function;
        cfg->name = name;
        cfg->result_type = std::move(result_type);
        cfg->max_command_execution_time_seconds = max_command_execution_time;
    };

    switch (func_type)
    {
        case UserDefinedFunctionConfiguration::FuncType::EXECUTABLE: {
            auto udf_config = std::make_shared<ExecutableUserDefinedFunctionConfiguration>();
            init_config(udf_config);
            udf_config->command = std::move(command);
            udf_config->command_arguments = std::move(command_arguments);
            udf_config->format = std::move(format);
            udf_config->command_termination_timeout_seconds = command_termination_timeout_seconds;
            udf_config->command_read_timeout_milliseconds = command_read_timeout_milliseconds;
            udf_config->command_write_timeout_milliseconds = command_write_timeout_milliseconds;
            udf_config->pool_size = pool_size;
            udf_config->is_executable_pool = true;
            udf_config->send_chunk_header = send_chunk_header;
            udf_config->execute_direct = execute_direct;
            return std::make_shared<UserDefinedExecutableFunction>(std::move(udf_config), lifetime);
        }
        case UserDefinedFunctionConfiguration::FuncType::REMOTE: {
            auto udf_config = std::make_shared<RemoteUserDefinedFunctionConfiguration>();
            init_config(udf_config);
            udf_config->command_read_timeout_milliseconds = command_read_timeout_milliseconds;
            udf_config->url = url;
            udf_config->auth_method = std::move(auth_method);
            udf_config->auth_context = std::move(auth_ctx);
            return std::make_shared<UserDefinedExecutableFunction>(std::move(udf_config), lifetime);
        }
        case UserDefinedFunctionConfiguration::FuncType::JAVASCRIPT: {
            auto udf_config = std::make_shared<JavaScriptUserDefinedFunctionConfiguration>();
            init_config(udf_config);
            udf_config->source = std::move(source);
            return std::make_shared<UserDefinedExecutableFunction>(std::move(udf_config), lifetime);
        }
        case UserDefinedFunctionConfiguration::FuncType::UNKNOWN:
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Wrong user defined function type expected 'executable', 'remote' or 'javascript' actual {}",
                type);
    }
    /// proton: ends
}

}
