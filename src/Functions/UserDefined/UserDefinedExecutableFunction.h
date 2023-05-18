#pragma once

/// proton: starts
#include <Interpreters/IExternalLoadable.h>
#include <Processors/Sources/ShellCommandSource.h>
#include "UserDefinedFunctionConfiguration.h"

#include <string>
/// proton: ends

namespace DB
{

class ShellCommandSourceCoordinator;

class UserDefinedExecutableFunction final : public IExternalLoadable
{
public:

    UserDefinedExecutableFunction(
        UserDefinedFunctionConfigurationPtr configuration_,
        const ExternalLoadableLifetime & lifetime_);

    const ExternalLoadableLifetime & getLifetime() const override
    {
        return lifetime;
    }

    std::string getLoadableName() const override
    {
        return configuration->name;
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
        return std::make_shared<UserDefinedExecutableFunction>(configuration, lifetime);
    }

    const UserDefinedFunctionConfigurationPtr & getConfiguration() const
    {
        return configuration;
    }

private:
    UserDefinedFunctionConfigurationPtr configuration;
    ExternalLoadableLifetime lifetime;
};
}
