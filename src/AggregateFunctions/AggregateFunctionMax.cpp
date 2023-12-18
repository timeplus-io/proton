#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include "config.h"

namespace DB
{
struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionMax(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMaxData>(name, argument_types, parameters, settings));
}

#if USE_ARG_MIN_MAX_FUNCS

AggregateFunctionPtr createAggregateFunctionArgMax(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMaxData>(name, argument_types, parameters, settings));
}

#endif

}

void registerAggregateFunctionsMax(AggregateFunctionFactory & factory)
{
    factory.registerFunction("max", createAggregateFunctionMax, AggregateFunctionFactory::CaseInsensitive);

    #if USE_ARG_MIN_MAX_FUNCS
    /// The functions below depend on the order of data.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("arg_max", { createAggregateFunctionArgMax, properties });
    #endif
}

}
