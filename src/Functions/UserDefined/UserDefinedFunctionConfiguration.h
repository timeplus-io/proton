#pragma once

#include <DataTypes/IDataType.h>
#include <Common/TypePromotion.h>

#include <Poco/URI.h>

#include <string>

namespace DB
{
struct UserDefinedFunctionConfiguration : public TypePromotion<UserDefinedFunctionConfiguration>
{
    /// proton: starts.
    enum FuncType
    {
        EXECUTABLE = 0,
        REMOTE = 1,
        JAVASCRIPT = 2,
        UNKNOWN = 999
    };
    /// 'type' can be 'executable', 'remote' or 'javascript'
    FuncType type;
    struct Argument
    {
        std::string name;
        DataTypePtr type;
    };
    std::vector<Argument> arguments;

    /// whether the UDF is a aggregation function
    bool is_aggregation = false;

    std::string name;
    /// proton: ends

    DataTypePtr result_type;

    /// Max command execution time in milliseconds. Valid only if executable_pool = true
    size_t max_command_execution_time_seconds = 10;

    UserDefinedFunctionConfiguration() = default;
    UserDefinedFunctionConfiguration(const UserDefinedFunctionConfiguration &) = default;
    virtual ~UserDefinedFunctionConfiguration() = default;
};

struct ExecutableUserDefinedFunctionConfiguration : public UserDefinedFunctionConfiguration
{
    /// executable file name, only available when 'type' is 'executable'
    std::string command;
    std::vector<std::string> command_arguments;

    /// Script output format
    std::string format;

    /// Command termination timeout in seconds
    size_t command_termination_timeout_seconds = 10;

    /// Timeout for reading data from command stdout
    size_t command_read_timeout_milliseconds = 10000;

    /// Timeout for writing data to command stdin
    size_t command_write_timeout_milliseconds = 10000;

    /// Pool size valid only if executable_pool = true
    size_t pool_size = 16;

    /// Should pool of processes be created.
    bool is_executable_pool = false;

    /// Send number_of_rows\n before sending chunk to process.
    bool send_chunk_header = false;

    /// Execute script direct or with /bin/bash.
    bool execute_direct = true;
};

struct RemoteUserDefinedFunctionConfiguration : public UserDefinedFunctionConfiguration
{
    /// Timeout for reading data from input format
    size_t command_read_timeout_milliseconds = 10000;

    /// Timeout for receiving response from remote endpoint
    size_t command_execution_timeout_milliseconds = 10000;

    /// url of remote endpoint, only available when 'type' is 'remote'
    Poco::URI url;
    enum AuthMethod
    {
        NONE = 0,
        AUTH_HEADER = 1
    };
    AuthMethod auth_method = AuthMethod::NONE;
    struct AuthContext
    {
        /// authorization header name, only available when 'type' is 'remote' and 'auth_method' is AUTH_HEADER
        std::string key_name;
        /// authorization header value, only available when 'type' is 'remote' and 'auth_method' is AUTH_HEADER
        std::string key_value;
    };
    AuthContext auth_context;
};

struct JavaScriptUserDefinedFunctionConfiguration : public UserDefinedFunctionConfiguration
{
    /// source code of function, only available when 'type' is 'javascript'
    std::string source;
};

using UserDefinedFunctionConfigurationPtr = std::shared_ptr<UserDefinedFunctionConfiguration>;
using JavaScriptUserDefinedFunctionConfigurationPtr = std::shared_ptr<JavaScriptUserDefinedFunctionConfiguration>;
}
