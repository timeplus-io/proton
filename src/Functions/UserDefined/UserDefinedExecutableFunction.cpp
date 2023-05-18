#include "UserDefinedExecutableFunction.h"

namespace DB
{

UserDefinedExecutableFunction::UserDefinedExecutableFunction(
    UserDefinedFunctionConfigurationPtr configuration_,
    const ExternalLoadableLifetime & lifetime_)
    : configuration(std::move(configuration_))
    , lifetime(lifetime_)
{
}

}
