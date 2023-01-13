#pragma once

#include <memory>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExternalLoader.h>
#include <Interpreters/UserDefinedExecutableFunction.h>

namespace DB
{

class IExternalLoaderConfigRepository;

/// Manages external user-defined functions.
class ExternalUserDefinedFunctionsLoader : public ExternalLoader, WithContext
{
public:

    using UserDefinedExecutableFunctionPtr = std::shared_ptr<const UserDefinedExecutableFunction>;

    /// proton: starts
    /// External user-defined functions will be loaded immediately and then will be updated in separate thread, each 'reload_period' seconds.
    explicit ExternalUserDefinedFunctionsLoader(ContextPtr global_context_);
    /// proton: ends

    UserDefinedExecutableFunctionPtr getUserDefinedFunction(const std::string & user_defined_function_name) const;

    UserDefinedExecutableFunctionPtr tryGetUserDefinedFunction(const std::string & user_defined_function_name) const;

    void reloadFunction(const std::string & user_defined_function_name) const;

    /// proton: starts
    static ExternalUserDefinedFunctionsLoader & instance(ContextPtr context);
    /// proton: ends

protected:
    LoadablePtr create(const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & key_in_config,
        const std::string & repository_name) const override;

private:
    mutable std::mutex external_user_defined_executable_functions_mutex;


};

}
