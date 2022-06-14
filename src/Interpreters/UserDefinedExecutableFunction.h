#pragma once

#include <string>

#include <DataTypes/IDataType.h>
#include <Processors/Sources/ShellCommandSource.h>
#include <Interpreters/IExternalLoadable.h>


namespace DB
{

struct UserDefinedExecutableFunctionConfiguration
{
    /// proton: starts.
    enum FuncType
    {
        EXECUTABLE = 0,
        REMOTE = 1,
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
    /// proton: ends
    std::string name;
    /// executable file name, only available when 'type' is 'remote'
    std::string command;
    std::vector<std::string> command_arguments;
    DataTypePtr result_type;
};

class UserDefinedExecutableFunction final : public IExternalLoadable
{
public:

    UserDefinedExecutableFunction(
        const UserDefinedExecutableFunctionConfiguration & configuration_,
        std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
        const ExternalLoadableLifetime & lifetime_);

    const ExternalLoadableLifetime & getLifetime() const override
    {
        return lifetime;
    }

    std::string getLoadableName() const override
    {
        return configuration.name;
    }

    bool supportUpdates() const override
    {
        return true;
    }

    bool isModified() const override
    {
        return true;
    }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<UserDefinedExecutableFunction>(configuration, coordinator, lifetime);
    }

    const UserDefinedExecutableFunctionConfiguration & getConfiguration() const
    {
        return configuration;
    }

    std::shared_ptr<ShellCommandSourceCoordinator> getCoordinator() const
    {
        return coordinator;
    }

    std::shared_ptr<UserDefinedExecutableFunction> shared_from_this()
    {
        return std::static_pointer_cast<UserDefinedExecutableFunction>(IExternalLoadable::shared_from_this());
    }

    std::shared_ptr<const UserDefinedExecutableFunction> shared_from_this() const
    {
        return std::static_pointer_cast<const UserDefinedExecutableFunction>(IExternalLoadable::shared_from_this());
    }

private:
    UserDefinedExecutableFunctionConfiguration configuration;
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator;
    ExternalLoadableLifetime lifetime;
};

}
