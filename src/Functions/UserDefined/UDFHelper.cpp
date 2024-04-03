#include <Functions/UserDefined/UDFHelper.h>

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/UserDefinedExecutableFunction.h>
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>
#include <Functions/UserDefined/PythonUserDefinedFunction.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/filesystemHelpers.h>

#include <Poco/JSON/Parser.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <boost/algorithm/string/split.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_REQUEST_PARAMETER;
extern const int UDF_INVALID_NAME;
extern const int FUNCTION_ALREADY_EXISTS;
extern const int CANNOT_DROP_FUNCTION;
extern const int UNKNOWN_FUNCTION;
extern const int UNSUPPORTED_METHOD;
extern const int UNKNOWN_TYPE;
}

namespace Streaming
{
/// Valid UDF function name is [a-zA-Z0-9_] and has to start with _ or [a-zA-Z]
void validateUDFName(const String & func_name)
{
    if (func_name.empty())
        throw Exception(ErrorCodes::UDF_INVALID_NAME, "UDF name must not be empy");

    /// At least one alphabetic
    bool has_alphabetic = false;
    for (auto ch : func_name)
    {
        if (!std::isalnum(ch) && ch != '_')
            throw Exception(ErrorCodes::UDF_INVALID_NAME, "UDF name can contain only alphabetic, numbers and underscores");

        has_alphabetic |= std::isalpha(ch);
    }

    if (!has_alphabetic)
        throw Exception(ErrorCodes::UDF_INVALID_NAME, "UDF name shall contain at lest one alphabetic");

    if (!std::isalpha(func_name[0]) && func_name[0] != '_')
        throw Exception(ErrorCodes::UDF_INVALID_NAME, "UDF name's first char shall be an alphabetic or underscore");

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(func_name))
        throw Exception(fmt::format("UDF name can not end up with {}, because it is key word suffix", combinator->getName()), ErrorCodes::UDF_INVALID_NAME);
}

namespace
{

UserDefinedFunctionConfiguration::FuncType getFuncType(String type_name)
{
    UserDefinedFunctionConfiguration::FuncType func_type;

    if (type_name == "executable")
        func_type = UserDefinedFunctionConfiguration::FuncType::EXECUTABLE;
    else if (type_name == "remote")
        func_type = UserDefinedFunctionConfiguration::FuncType::REMOTE;
    else if (type_name == "javascript")
        func_type = UserDefinedFunctionConfiguration::FuncType::JAVASCRIPT;
    else if (type_name == "python")
        func_type = UserDefinedFunctionConfiguration::FuncType::PYTHON;
    else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Wrong user defined function type expected 'executable' or 'remote' actual {}", type_name);

    return func_type;
}
}

UserDefinedExecutableFunctionPtr
createUserDefinedExecutableFunction(ContextPtr context, const std::string & name, const Poco::Util::AbstractConfiguration & config)
{
    const String key_in_config = "function";
    auto get_or_throw = [&](String key) -> String {
        String value = config.getString("function." + key, "");
        if (value.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing '{}' for user defined function ", key);
        return value;
    };

    String type = get_or_throw("type");
    UserDefinedFunctionConfiguration::FuncType func_type = getFuncType(type);

    ExternalLoadableLifetime lifetime;
    if (config.has(key_in_config + ".lifetime"))
        lifetime = ExternalLoadableLifetime(config, key_in_config + ".lifetime");

    auto init_config = [&](UserDefinedFunctionConfigurationPtr cfg) {
        /// Throw exception if missing 'return_type'
        DataTypePtr result_type = DataTypeFactory::instance().get(get_or_throw("return_type"));
        bool is_aggr_function = config.getBool(key_in_config + ".is_aggregation", false);

        size_t max_command_execution_time = config.getUInt64(key_in_config + ".max_command_execution_time", 10);
        size_t max_execution_time_seconds = static_cast<size_t>(context->getSettings().max_execution_time.totalSeconds());
        if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
            max_command_execution_time = max_execution_time_seconds;

        /// Get arguments from config
        /// Below implementation only available for JSON configuration, because Poco::Util::AbstractConfiguration cannot work well with Array
        std::vector<UserDefinedFunctionConfiguration::Argument> arguments;
        String arg_str = config.getRawString(key_in_config + ".arguments", "");
        if (arg_str.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Missing argument definition.");

        if (!arg_str.empty())
        {
            Poco::JSON::Parser parser;
            try
            {
                auto json_arguments = parser.parse(arg_str).extract<Poco::JSON::Array::Ptr>();

                if (json_arguments->size() < 1)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Empty input argument is not supported.");

                for (unsigned int i = 0; i < json_arguments->size(); i++)
                {
                    const auto & arg = json_arguments->getObject(i);
                    if (!arg)
                        throw Exception(
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Invalid argument, `arguments` are expected to contain 'name' and 'type' information.");

                    if (!arg->has("name") || !arg->has("type"))
                        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid argument, missing argument 'name or 'type'.");

                    UserDefinedFunctionConfiguration::Argument argument;
                    argument.name = arg->get("name").toString();
                    const auto arg_type = arg->get("type").toString();
                    try
                    {
                        argument.type = DataTypeFactory::instance().get(arg_type);
                    }
                    catch (Exception & e)
                    {
                        if (e.code() == ErrorCodes::UNKNOWN_TYPE)
                            e.addMessage(fmt::format("Invalid argument type '{}'", argument.name, arg_type));
                        throw;
                    }
                    arguments.emplace_back(std::move(argument));
                }
            }
            catch (const std::exception & e)
            {
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid UDF config: {}", e.what());
            }
        }

        cfg->type = std::move(func_type);
        cfg->arguments = std::move(arguments);
        cfg->is_aggregation = is_aggr_function;
        cfg->name = name;
        cfg->result_type = std::move(result_type);
        cfg->max_command_execution_time_seconds = max_command_execution_time;
    };

    auto init_executable_func_config = [&](std::shared_ptr<ExecutableUserDefinedFunctionConfiguration> & cfg) {
        init_config(cfg);

        /// read settings from config
        String format = config.getString(key_in_config + ".format", "ArrowStream");

        bool send_chunk_header = config.getBool(key_in_config + ".send_chunk_header", false);
        size_t command_termination_timeout_seconds = config.getUInt64(key_in_config + ".command_termination_timeout", 0);
        size_t command_read_timeout_milliseconds = config.getUInt64(key_in_config + ".command_read_timeout", 10000);
        size_t command_write_timeout_milliseconds = config.getUInt64(key_in_config + ".command_write_timeout", 10000);

        size_t pool_size = 0;
        pool_size = config.getUInt64(key_in_config + ".pool_size", 1);

        bool execute_direct = config.getBool(key_in_config + ".execute_direct", true);
        std::vector<String> command_arguments;
        if (execute_direct)
        {
            /// Throw exception if missing 'command'
            boost::split(command_arguments, get_or_throw("command"), [](char c) { return c == ' '; });
            String command_value = std::move(command_arguments[0]);

            auto user_scripts_path = context->getUserScriptsPath();
            String command = std::filesystem::path(user_scripts_path) / command_value;

            if (!fileOrSymlinkPathStartsWith(command, user_scripts_path))
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable file {} must be inside user scripts folder {}",
                    command_value,
                    user_scripts_path);

            if (!std::filesystem::exists(std::filesystem::path(command)))
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Executable file {} does not exist inside user scripts folder {}",
                    command_value,
                    user_scripts_path);

            command_arguments.erase(command_arguments.begin());

            cfg->command = std::move(command);
            cfg->command_arguments = std::move(command_arguments);
        }

        cfg->command_arguments = std::move(command_arguments);
        cfg->format = std::move(format);
        cfg->command_termination_timeout_seconds = command_termination_timeout_seconds;
        cfg->command_read_timeout_milliseconds = command_read_timeout_milliseconds;
        cfg->command_write_timeout_milliseconds = command_write_timeout_milliseconds;
        cfg->pool_size = pool_size;
        cfg->is_executable_pool = true;
        cfg->send_chunk_header = send_chunk_header;
        cfg->execute_direct = execute_direct;
    };

    auto init_remote_func_config = [&](std::shared_ptr<RemoteUserDefinedFunctionConfiguration> & cfg) {
        init_config(cfg);

        /// Get auth settings for RemoteUserDefinedFunctionConfiguration and validate auth_method
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
            if (auth_ctx.key_name.empty() || auth_ctx.key_value.empty())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "'auth_header' auth_method requires 'key_name' and 'key_value' in 'auth_context'");
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong 'auth_method' expected 'none' or 'auth_header' actual {}", method);

        /// Throws exception if 'url' missing for remote UDF
        String url_value = get_or_throw("url");

        try
        {
            Poco::URI url(url_value);
            cfg->url = url;
        }
        catch (const Poco::SyntaxException & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid url for remote UDF, msg: {}", e.message());
        }
        cfg->command_read_timeout_milliseconds = config.getUInt64(key_in_config + ".command_read_timeout", 10000);
        cfg->auth_method = std::move(auth_method);
        cfg->auth_context = std::move(auth_ctx);
    };

    switch (func_type)
    {
        case UserDefinedFunctionConfiguration::FuncType::EXECUTABLE: {
            auto udf_config = std::make_shared<ExecutableUserDefinedFunctionConfiguration>();
            init_executable_func_config(udf_config);
            return std::make_shared<UserDefinedExecutableFunction>(std::move(udf_config), lifetime);
        }
        case UserDefinedFunctionConfiguration::FuncType::REMOTE: {
            auto udf_config = std::make_shared<RemoteUserDefinedFunctionConfiguration>();
            init_remote_func_config(udf_config);
            return std::make_shared<UserDefinedExecutableFunction>(std::move(udf_config), lifetime);
        }
        case UserDefinedFunctionConfiguration::FuncType::JAVASCRIPT: {
            auto udf_config = std::make_shared<JavaScriptUserDefinedFunctionConfiguration>();
            init_config(udf_config);
            String source = get_or_throw("source");
            udf_config->source = std::move(source);
            return std::make_shared<UserDefinedExecutableFunction>(std::move(udf_config), lifetime);
        }
        case UserDefinedFunctionConfiguration::FuncType::PYTHON: {
            auto udf_config = std::make_shared<PythonUserDefinedFunctionConfiguration>();
            init_config(udf_config);
            String source = get_or_throw("source");
            udf_config->source = std::move(source);
            return std::make_shared<UserDefinedExecutableFunction>(std::move(udf_config), lifetime);
        }

        case UserDefinedFunctionConfiguration::FuncType::UNKNOWN:
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Wrong user defined function type expected 'executable', 'remote' or 'javascript' actual {}",
                type);
    }
}
}
}
