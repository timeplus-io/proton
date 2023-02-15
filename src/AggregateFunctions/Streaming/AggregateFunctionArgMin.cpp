#include "config.h"

#if USE_ARG_MIN_MAX_FUNCS

#include "HelpersArgMinMax.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>

namespace DB
{
namespace Streaming
{
namespace
{
AggregateFunctionPtr createAggregateFunctionArgMin(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionCountedArgMinMax<false>(name, argument_types, parameters, settings));
}
}

void registerAggregateFunctionsArgMinRetract(AggregateFunctionFactory & factory)
{
    /// The functions below depend on the order of data.
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("__arg_min_retract", {createAggregateFunctionArgMin, properties});
}
}
}

#endif
