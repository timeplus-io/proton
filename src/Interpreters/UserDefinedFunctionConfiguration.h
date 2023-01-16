#pragma once

#include <DataTypes/IDataType.h>

#include <Poco/URI.h>

#include <string>

namespace DB
{
struct UserDefinedFunctionConfiguration
{
    /// proton: starts.
    enum FuncType
    {
        EXECUTABLE = 0,
        REMOTE = 1,
        JAVASCRIPT = 2,
        UNKNOWN = 999
    };
    /// 'type' can be 'executable' or 'remote'
    FuncType type;
    /// url of remote endpoint, only available when 'type' is 'remote'
    Poco::URI url;
    enum AuthMethod
    {
        NONE = 0,
        AUTH_HEADER = 1
    };
    AuthMethod auth_method;
    struct AuthContext
    {
        /// authorization header name, only available when 'type' is 'remote' and 'auth_method' is AUTH_HEADER
        std::string key_name;
        /// authorization header value, only available when 'type' is 'remote' and 'auth_method' is AUTH_HEADER
        std::string key_value;
    };
    AuthContext auth_context;
    struct Argument
    {
        std::string name;
        DataTypePtr type;
    };
    std::vector<Argument> arguments;
    /// source code of function, only available when 'type' is 'javascript'
    std::string source;
    /// whether the UDF is a aggregation function
    bool is_aggregation = false;

    /// proton: ends
    std::string name;
    /// executable file name, only available when 'type' is 'executable'
    std::string command;
    std::vector<std::string> command_arguments;
    DataTypePtr result_type;
};
}
